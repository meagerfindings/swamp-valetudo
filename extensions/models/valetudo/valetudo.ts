/**
 * Capability-aware Valetudo control and status over MQTT.
 *
 * The model follows Valetudo's Homie metadata instead of assuming that every
 * robot exposes the same features or preset values.
 *
 * @module
 */

import { connectAsync, type MqttClient } from "npm:mqtt@5.15.2";
import { z } from "npm:zod@4";

const GlobalArgsSchema = z.object({
  brokerUrl: z.string().url().superRefine((value, context) => {
    const url = new URL(value);
    if (!["mqtt:", "mqtts:", "ws:", "wss:"].includes(url.protocol)) {
      context.addIssue({
        code: "custom",
        message: "Broker URL must use mqtt, mqtts, ws, or wss",
      });
    }
    if (url.username || url.password) {
      context.addIssue({
        code: "custom",
        message:
          "Broker URL must not contain credentials; use username and password arguments",
      });
    }
  }).describe("MQTT broker URL, such as mqtt://broker:1883"),
  username: z.string().optional().describe("Optional MQTT username"),
  password: z.string().optional().meta({ sensitive: true }).describe(
    "Optional MQTT password; use a Swamp vault expression",
  ),
  topicPrefix: z.string().regex(
    /^[A-Za-z0-9_-]+(?:\/[A-Za-z0-9_-]+)*$/,
    "Topic prefix must contain only safe MQTT path segments",
  ).default("valetudo").describe(
    "Valetudo MQTT topic prefix",
  ),
  identifier: z.string().regex(
    /^[A-Za-z0-9_-]+$/,
    "Identifier must be one safe MQTT topic segment",
  ).describe("Valetudo MQTT robot identifier"),
  connectTimeoutMs: z.number().int().min(1000).max(60000).default(10000),
  readTimeoutMs: z.number().int().min(250).max(30000).default(3000),
  publishTimeoutMs: z.number().int().min(250).max(30000).default(5000),
});

const PropertySchema = z.object({
  id: z.string(),
  name: z.string().nullable(),
  datatype: z.string().nullable(),
  settable: z.boolean(),
  retained: z.boolean(),
  format: z.string().nullable(),
  unit: z.string().nullable(),
});

const CapabilitySchema = z.object({
  id: z.string(),
  name: z.string().nullable(),
  properties: z.array(PropertySchema),
});

const DiscoverySchema = z.object({
  baseTopic: z.string(),
  homieVersion: z.string().nullable(),
  implementation: z.string().nullable(),
  state: z.string().nullable(),
  capabilities: z.array(CapabilitySchema),
  discoveredAt: z.string(),
});

const StatusSchema = z.object({
  mqttState: z.string().nullable(),
  robotStatus: z.string().nullable(),
  statusFlag: z.string().nullable(),
  errorDescription: z.string().nullable(),
  batteryLevel: z.number().int().nullable(),
  batteryStatus: z.string().nullable(),
  dockStatus: z.string().nullable(),
  dustbinAttached: z.boolean().nullable(),
  mopAttached: z.boolean().nullable(),
  waterTankAttached: z.boolean().nullable(),
  fanPreset: z.string().nullable(),
  waterPreset: z.string().nullable(),
  operationMode: z.string().nullable(),
  segments: z.record(z.string(), z.string()),
  events: z.record(z.string(), z.unknown()),
  observedAt: z.string(),
});

const ActionArgumentsSchema = z.object({
  action: z.enum([
    "start",
    "stop",
    "pause",
    "home",
    "locate",
    "auto-empty",
    "clean-segments",
    "set-property",
  ]),
  segmentIds: z.array(z.string().min(1)).min(1).optional().describe(
    "Segment IDs for clean-segments",
  ),
  iterations: z.number().int().min(1).max(3).default(1),
  customOrder: z.boolean().optional(),
  capability: z.string().optional().describe("Capability ID for set-property"),
  property: z.string().optional().describe("Property ID for set-property"),
  value: z.string().optional().describe("Property payload for set-property"),
});

const ActionResultSchema = z.object({
  action: z.string(),
  topic: z.string(),
  payload: z.string(),
  publishedAt: z.string(),
});

/** Resolved model configuration after Zod defaults and validation. */
export type GlobalArgs = z.infer<typeof GlobalArgsSchema>;
type ActionArguments = z.infer<typeof ActionArgumentsSchema>;

interface Logger {
  info: (message: string, properties?: Record<string, unknown>) => void;
}

interface MethodContext {
  globalArgs: GlobalArgs;
  logger: Logger;
  writeResource: (
    specName: string,
    instanceName: string,
    data: unknown,
  ) => Promise<unknown>;
}

interface CheckContext {
  globalArgs: GlobalArgs;
}

/** MQTT capability/property command derived from a typed action request. */
export interface ActionCommand {
  capability: string;
  property: string;
  payload: string;
}

/** Homie metadata required to authorize and validate a command. */
export interface PropertyMetadata {
  datatype: string;
  settable: boolean;
  format: string | null;
}

const METADATA_FIELDS = [
  "$name",
  "$datatype",
  "$settable",
  "$retained",
  "$format",
  "$unit",
] as const;

/** Return the normalized MQTT base topic for one robot. */
function baseTopic(args: GlobalArgs): string {
  return `${args.topicPrefix}/${args.identifier}`;
}

/** Split a Homie comma-delimited list, removing empty entries. */
export function parseHomieList(value: string | undefined): string[] {
  return (value ?? "").split(",").map((item) => item.trim()).filter(Boolean);
}

/** Parse a retained JSON object and identify malformed source data. */
function parseObject(
  value: string | undefined,
  topic: string,
): Record<string, unknown> {
  if (!value) return {};
  try {
    const parsed = JSON.parse(value);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("expected a JSON object");
    }
    return parsed as Record<string, unknown>;
  } catch (error) {
    throw new Error(
      `Invalid retained JSON on ${topic}: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
}

/** Parse a retained integer, returning null for missing or invalid values. */
function parseInteger(value: string | undefined): number | null {
  if (value === undefined || !/^-?\d+$/.test(value)) return null;
  return Number.parseInt(value, 10);
}

/** Parse Valetudo/Homie boolean values. */
function parseBoolean(value: string | undefined): boolean | null {
  if (value === undefined) return null;
  if (["true", "on", "1"].includes(value.toLowerCase())) return true;
  if (["false", "off", "0"].includes(value.toLowerCase())) return false;
  return null;
}

/** Reject MQTT wildcards and invalid topic segments. */
function validateTopicPath(
  value: string,
  label: string,
  allowPath = false,
): void {
  const pattern = allowPath
    ? /^[A-Za-z0-9_-]+(?:\/[A-Za-z0-9_-]+)*$/
    : /^[A-Za-z0-9_-]+$/;
  if (
    !pattern.test(value) ||
    value.includes("..")
  ) {
    throw new Error(
      `${label} must contain only safe MQTT topic ${
        allowPath ? "path segments" : "segment characters"
      }`,
    );
  }
}

/** Parse and validate IDs before interpolating broker-provided metadata. */
function validatedHomieList(
  value: string | undefined,
  label: string,
  allowPath = false,
): string[] {
  const items = parseHomieList(value);
  for (const item of items) validateTopicPath(item, label, allowPath);
  return items;
}

/** Convert a requested action into its Valetudo capability command. */
export function buildActionCommand(args: ActionArguments): ActionCommand {
  const controls: Record<string, string> = {
    start: "START",
    stop: "STOP",
    pause: "PAUSE",
    home: "HOME",
  };
  if (controls[args.action]) {
    return {
      capability: "BasicControlCapability",
      property: "operation",
      payload: controls[args.action],
    };
  }
  if (args.action === "locate") {
    return {
      capability: "LocateCapability",
      property: "locate",
      payload: "PERFORM",
    };
  }
  if (args.action === "auto-empty") {
    return {
      capability: "AutoEmptyDockManualTriggerCapability",
      property: "trigger",
      payload: "PERFORM",
    };
  }
  if (args.action === "clean-segments") {
    if (!args.segmentIds?.length) {
      throw new Error("clean-segments requires at least one segment ID");
    }
    return {
      capability: "MapSegmentationCapability",
      property: "clean",
      payload: JSON.stringify({
        segment_ids: args.segmentIds,
        iterations: args.iterations,
        ...(args.customOrder === undefined
          ? {}
          : { customOrder: args.customOrder }),
      }),
    };
  }
  if (!args.capability || !args.property || args.value === undefined) {
    throw new Error(
      "set-property requires capability, property, and value inputs",
    );
  }
  validateTopicPath(args.capability, "capability");
  validateTopicPath(args.property, "property", true);
  return {
    capability: args.capability,
    property: args.property,
    payload: args.value,
  };
}

/** Connect to MQTT with retries disabled so failures return promptly. */
async function connectClient(args: GlobalArgs): Promise<MqttClient> {
  return await connectAsync(args.brokerUrl, {
    username: args.username,
    password: args.password,
    connectTimeout: args.connectTimeoutMs,
    reconnectPeriod: 0,
    clean: true,
    clientId: `swamp-valetudo-${crypto.randomUUID().slice(0, 8)}`,
  }, false);
}

/** Reject an MQTT operation that does not settle before its deadline. */
async function withDeadline<T>(
  operation: Promise<T>,
  timeoutMs: number,
  label: string,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      operation,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(
          () => reject(new Error(`${label} timed out after ${timeoutMs} ms`)),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

/** Close an MQTT client on success and failure paths. */
export async function withClient<T>(
  args: GlobalArgs,
  operation: (client: MqttClient) => Promise<T>,
  connector: (args: GlobalArgs) => Promise<MqttClient> = connectClient,
): Promise<T> {
  const client = await connector(args);
  try {
    return await operation(client);
  } finally {
    await client.endAsync(true);
  }
}

/**
 * Read retained values for exact topics and clean up every listener and timer.
 * Required topics cause a rejection when absent; optional topics are collected
 * until all expected values arrive or the post-required quiet period elapses.
 */
export async function readRetained(
  client: MqttClient,
  topics: string[],
  timeoutMs: number,
  requiredTopics: string[] = [],
): Promise<Map<string, string>> {
  const values = new Map<string, string>();
  if (topics.length === 0) return values;

  const expected = new Set(topics);
  const required = new Set(requiredTopics);
  for (const topic of required) {
    if (!expected.has(topic)) {
      throw new Error(`Required topic is not in subscription: ${topic}`);
    }
  }

  return await new Promise<Map<string, string>>((resolve, reject) => {
    let settled = false;
    let quietTimer: ReturnType<typeof setTimeout> | undefined;

    const cleanup = () => {
      if (quietTimer) clearTimeout(quietTimer);
      clearTimeout(timeoutTimer);
      client.removeListener("message", onMessage);
    };
    const finish = (error?: Error) => {
      if (settled) return;
      settled = true;
      cleanup();
      if (error) reject(error);
      else resolve(values);
    };
    const requiredReceived = () =>
      [...required].every((topic) => values.has(topic));
    const maybeFinish = () => {
      if (!requiredReceived()) return;
      if (values.size === expected.size) {
        finish();
        return;
      }
      if (quietTimer) clearTimeout(quietTimer);
      quietTimer = setTimeout(() => finish(), 150);
    };
    const onMessage = (topic: string, payload: Uint8Array) => {
      if (expected.has(topic)) {
        values.set(topic, new TextDecoder().decode(payload));
        maybeFinish();
      }
    };

    client.on("message", onMessage);
    const timeoutTimer = setTimeout(() => {
      const missing = [...required].filter((topic) => !values.has(topic));
      if (missing.length > 0) {
        finish(
          new Error(
            `Timed out waiting for retained topics: ${missing.join(", ")}`,
          ),
        );
      } else {
        finish();
      }
    }, timeoutMs);

    client.subscribeAsync(topics, { qos: 0 }).then(maybeFinish).catch((error) =>
      finish(
        new Error(
          `MQTT subscribe failed: ${
            error instanceof Error ? error.message : String(error)
          }`,
        ),
      )
    );
  });
}

/** Confirm a property is currently advertised and read its command metadata. */
export async function readPropertyMetadata(
  client: MqttClient,
  base: string,
  capability: string,
  property: string,
  timeoutMs: number,
): Promise<PropertyMetadata> {
  const propertyBase = `${base}/${capability}/${property}`;
  const nodesTopic = `${base}/$nodes`;
  const propertiesTopic = `${base}/${capability}/$properties`;
  const datatypeTopic = `${propertyBase}/$datatype`;
  const settableTopic = `${propertyBase}/$settable`;
  const formatTopic = `${propertyBase}/$format`;
  const topics = [
    nodesTopic,
    propertiesTopic,
    datatypeTopic,
    settableTopic,
    formatTopic,
  ];
  const values = await readRetained(client, topics, timeoutMs, [
    nodesTopic,
    propertiesTopic,
    datatypeTopic,
    settableTopic,
  ]);
  const nodes = validatedHomieList(values.get(nodesTopic), "$nodes capability");
  if (!nodes.includes(capability)) {
    throw new Error(`Valetudo does not currently advertise ${capability}`);
  }
  const properties = validatedHomieList(
    values.get(propertiesTopic),
    `${capability} property`,
    true,
  );
  if (!properties.includes(property)) {
    throw new Error(
      `Valetudo does not currently advertise ${capability}/${property}`,
    );
  }
  const datatype = values.get(datatypeTopic);
  if (!datatype) {
    throw new Error(
      `Valetudo did not advertise a datatype for ${capability}/${property}`,
    );
  }
  return {
    datatype,
    settable: values.get(settableTopic) === "true",
    format: values.get(formatTopic) ?? null,
  };
}

/** Validate a command against the property metadata advertised by Valetudo. */
export function validatePayload(
  metadata: PropertyMetadata,
  payload: string,
): void {
  if (!metadata.settable) {
    throw new Error("Valetudo does not advertise this property as settable");
  }
  if (metadata.datatype === "enum") {
    const allowed = metadata.format?.split(",") ?? [];
    if (allowed.length === 0 || allowed.some((value) => value.length === 0)) {
      throw new Error("Valetudo advertised an enum without a valid format");
    }
    if (!allowed.includes(payload)) {
      throw new Error(
        `Payload is not allowed; expected one of: ${allowed.join(", ")}`,
      );
    }
    return;
  }
  if (metadata.datatype === "integer") {
    if (!/^-?\d+$/.test(payload)) {
      throw new Error("Payload must be an integer");
    }
    const numeric = BigInt(payload);
    const int64Min = -(2n ** 63n);
    const int64Max = 2n ** 63n - 1n;
    if (numeric < int64Min || numeric > int64Max) {
      throw new Error("Payload must fit in a signed 64-bit integer");
    }
    if (metadata.format !== null) {
      const match = metadata.format.match(/^(-?\d+):(-?\d+)$/);
      if (!match) {
        throw new Error("Valetudo advertised an invalid integer format");
      }
      const minimum = BigInt(match[1]);
      const maximum = BigInt(match[2]);
      if (
        minimum < int64Min || maximum > int64Max || minimum > maximum
      ) {
        throw new Error("Valetudo advertised an invalid integer range");
      }
      if (numeric < minimum || numeric > maximum) {
        throw new Error(
          `Payload must be an integer in range ${metadata.format}`,
        );
      }
    }
    return;
  }
  if (metadata.datatype === "float") {
    if (
      !/^-?(?:\d+\.?\d*|\.\d+)$/.test(payload) ||
      !Number.isFinite(Number(payload))
    ) {
      throw new Error("Payload must be a finite decimal number");
    }
    if (metadata.format !== null) {
      const match = metadata.format.match(
        /^(-?(?:\d+\.?\d*|\.\d+)):(-?(?:\d+\.?\d*|\.\d+))$/,
      );
      if (!match) {
        throw new Error("Valetudo advertised an invalid float format");
      }
      const minimum = Number(match[1]);
      const maximum = Number(match[2]);
      const numeric = Number(payload);
      if (
        !Number.isFinite(minimum) || !Number.isFinite(maximum) ||
        minimum > maximum
      ) {
        throw new Error("Valetudo advertised an invalid float range");
      }
      if (numeric < minimum || numeric > maximum) {
        throw new Error(`Payload must be a number in range ${metadata.format}`);
      }
    }
    return;
  }
  if (metadata.datatype === "boolean") {
    if (!["true", "false"].includes(payload)) {
      throw new Error("Payload must be true or false");
    }
    return;
  }
  if (metadata.datatype !== "string") {
    throw new Error(`Unsupported Homie datatype: ${metadata.datatype}`);
  }
}

/** Validate command metadata and publish one non-retained QoS 1 action. */
export async function publishCommand(
  client: MqttClient,
  base: string,
  command: ActionCommand,
  readTimeoutMs: number,
  publishTimeoutMs: number,
): Promise<string> {
  const topic = `${base}/${command.capability}/${command.property}/set`;
  const metadata = await readPropertyMetadata(
    client,
    base,
    command.capability,
    command.property,
    readTimeoutMs,
  );
  validatePayload(metadata, command.payload);
  await withDeadline(
    client.publishAsync(topic, command.payload, {
      qos: 1,
      retain: false,
    }),
    publishTimeoutMs,
    `MQTT publish to ${topic}`,
  );
  return topic;
}

/** Swamp model definition for Valetudo robots connected through MQTT. */
export const model = {
  type: "@mgreten/valetudo",
  version: "2026.07.24.1",
  globalArguments: GlobalArgsSchema,
  resources: {
    discovery: {
      description: "Valetudo Homie capability and property metadata",
      schema: DiscoverySchema,
      lifetime: "7d",
      garbageCollection: 5,
    },
    status: {
      description: "Current retained Valetudo robot status",
      schema: StatusSchema,
      lifetime: "7d",
      garbageCollection: 20,
    },
    actionResult: {
      description: "A command successfully validated and published to Valetudo",
      schema: ActionResultSchema,
      lifetime: "7d",
      garbageCollection: 20,
    },
  },
  checks: {
    "mqtt-connectivity": {
      description: "Verify the MQTT broker is reachable before publishing",
      labels: ["live"],
      appliesTo: ["action"],
      execute: async (context: CheckContext) => {
        try {
          await withClient(
            context.globalArgs,
            () => Promise.resolve(undefined),
          );
          return { pass: true };
        } catch (error) {
          return {
            pass: false,
            errors: [
              `Cannot connect to MQTT broker: ${
                error instanceof Error ? error.message : String(error)
              }`,
            ],
          };
        }
      },
    },
  },
  methods: {
    discover: {
      description:
        "Discover the capabilities and properties advertised by Valetudo",
      arguments: z.object({}),
      execute: async (_args: Record<string, never>, context: MethodContext) => {
        const base = baseTopic(context.globalArgs);
        context.logger.info(
          "Discovering Valetudo capabilities at {baseTopic}",
          {
            baseTopic: base,
          },
        );
        const discovery = await withClient(
          context.globalArgs,
          async (client) => {
            const rootTopics = ["$homie", "$implementation", "$state", "$nodes"]
              .map((field) => `${base}/${field}`);
            const root = await readRetained(
              client,
              rootTopics,
              context.globalArgs.readTimeoutMs,
              [`${base}/$nodes`],
            );
            const nodeIds = validatedHomieList(
              root.get(`${base}/$nodes`),
              "$nodes capability",
            );
            if (nodeIds.length === 0) {
              throw new Error(
                `No retained Homie $nodes metadata found under ${base}; verify the topic prefix and identifier`,
              );
            }

            const nodeTopics = nodeIds.flatMap((node) => [
              `${base}/${node}/$name`,
              `${base}/${node}/$properties`,
            ]);
            const nodes = await readRetained(
              client,
              nodeTopics,
              context.globalArgs.readTimeoutMs,
              nodeIds.map((node) => `${base}/${node}/$properties`),
            );
            const propertyTopics = nodeIds.flatMap((node) =>
              validatedHomieList(
                nodes.get(`${base}/${node}/$properties`),
                `${node} property`,
                true,
              ).flatMap((property) =>
                METADATA_FIELDS.map((field) =>
                  `${base}/${node}/${property}/${field}`
                )
              )
            );
            const properties = await readRetained(
              client,
              propertyTopics,
              context.globalArgs.readTimeoutMs,
            );

            return {
              baseTopic: base,
              homieVersion: root.get(`${base}/$homie`) ?? null,
              implementation: root.get(`${base}/$implementation`) ?? null,
              state: root.get(`${base}/$state`) ?? null,
              capabilities: nodeIds.map((node) => ({
                id: node,
                name: nodes.get(`${base}/${node}/$name`) ?? null,
                properties: validatedHomieList(
                  nodes.get(`${base}/${node}/$properties`),
                  `${node} property`,
                  true,
                ).map((property) => {
                  const propertyBase = `${base}/${node}/${property}`;
                  return {
                    id: property,
                    name: properties.get(`${propertyBase}/$name`) ?? null,
                    datatype: properties.get(`${propertyBase}/$datatype`) ??
                      null,
                    settable: parseBoolean(
                      properties.get(`${propertyBase}/$settable`),
                    ) ===
                      true,
                    retained: parseBoolean(
                      properties.get(`${propertyBase}/$retained`),
                    ) ===
                      true,
                    format: properties.get(`${propertyBase}/$format`) ?? null,
                    unit: properties.get(`${propertyBase}/$unit`) ?? null,
                  };
                }),
              })),
              discoveredAt: new Date().toISOString(),
            };
          },
        );

        const propertyCount = discovery.capabilities.reduce(
          (count, capability) => count + capability.properties.length,
          0,
        );
        context.logger.info(
          "Discovered {capabilityCount} capabilities and {propertyCount} properties",
          {
            capabilityCount: discovery.capabilities.length,
            propertyCount,
          },
        );
        const handle = await context.writeResource(
          "discovery",
          "valetudo-discovery",
          discovery,
        );
        return { dataHandles: [handle] };
      },
    },

    getStatus: {
      description:
        "Read a normalized snapshot from Valetudo's retained MQTT state",
      arguments: z.object({}),
      execute: async (_args: Record<string, never>, context: MethodContext) => {
        const base = baseTopic(context.globalArgs);
        context.logger.info("Reading Valetudo status at {baseTopic}", {
          baseTopic: base,
        });
        const suffixes = [
          "$state",
          "StatusStateAttribute/status",
          "StatusStateAttribute/flag",
          "StatusStateAttribute/error_description",
          "BatteryStateAttribute/level",
          "BatteryStateAttribute/status",
          "DockStatusStateAttribute/status",
          "AttachmentStateAttribute/dustbin",
          "AttachmentStateAttribute/mop",
          "AttachmentStateAttribute/watertank",
          "FanSpeedControlCapability/preset",
          "WaterUsageControlCapability/preset",
          "OperationModeControlCapability/preset",
          "MapData/segments",
          "ValetudoEvents/valetudo_events",
        ];
        const values = await withClient(
          context.globalArgs,
          (client) =>
            readRetained(
              client,
              suffixes.map((suffix) => `${base}/${suffix}`),
              context.globalArgs.readTimeoutMs,
            ),
        );
        const value = (suffix: string) => values.get(`${base}/${suffix}`);
        if (!value("$state") && !value("StatusStateAttribute/status")) {
          throw new Error(
            `No retained Valetudo status found under ${base}; verify MQTT connectivity and model configuration`,
          );
        }
        const segments = Object.fromEntries(
          Object.entries(
            parseObject(value("MapData/segments"), `${base}/MapData/segments`),
          ).map(([id, name]) => [id, String(name)]),
        );
        const status = {
          mqttState: value("$state") ?? null,
          robotStatus: value("StatusStateAttribute/status") ?? null,
          statusFlag: value("StatusStateAttribute/flag") ?? null,
          errorDescription: value("StatusStateAttribute/error_description") ??
            null,
          batteryLevel: parseInteger(value("BatteryStateAttribute/level")),
          batteryStatus: value("BatteryStateAttribute/status") ?? null,
          dockStatus: value("DockStatusStateAttribute/status") ?? null,
          dustbinAttached: parseBoolean(
            value("AttachmentStateAttribute/dustbin"),
          ),
          mopAttached: parseBoolean(value("AttachmentStateAttribute/mop")),
          waterTankAttached: parseBoolean(
            value("AttachmentStateAttribute/watertank"),
          ),
          fanPreset: value("FanSpeedControlCapability/preset") ?? null,
          waterPreset: value("WaterUsageControlCapability/preset") ?? null,
          operationMode: value("OperationModeControlCapability/preset") ?? null,
          segments,
          events: parseObject(
            value("ValetudoEvents/valetudo_events"),
            `${base}/ValetudoEvents/valetudo_events`,
          ),
          observedAt: new Date().toISOString(),
        };
        context.logger.info("Read Valetudo status {robotStatus}", {
          robotStatus: status.robotStatus ?? "unknown",
        });
        const handle = await context.writeResource(
          "status",
          "valetudo-status",
          status,
        );
        return { dataHandles: [handle] };
      },
    },

    action: {
      description: "Validate and publish a supported Valetudo robot action",
      arguments: ActionArgumentsSchema,
      execute: async (args: ActionArguments, context: MethodContext) => {
        const command = buildActionCommand(args);
        const base = baseTopic(context.globalArgs);
        const topic = `${base}/${command.capability}/${command.property}/set`;
        context.logger.info("Validating Valetudo {action} action", {
          action: args.action,
        });
        await withClient(context.globalArgs, (client) =>
          publishCommand(
            client,
            base,
            command,
            context.globalArgs.readTimeoutMs,
            context.globalArgs.publishTimeoutMs,
          ));
        const result = {
          action: args.action,
          topic,
          payload: command.payload,
          publishedAt: new Date().toISOString(),
        };
        context.logger.info("Published Valetudo {action} action", {
          action: args.action,
        });
        const handle = await context.writeResource(
          "actionResult",
          "valetudo-action-result",
          result,
        );
        return { dataHandles: [handle] };
      },
    },
  },
};
