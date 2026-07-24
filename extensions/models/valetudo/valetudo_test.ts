import { EventEmitter } from "node:events";
import type { MqttClient } from "npm:mqtt@5.15.2";
import { assertEquals, assertRejects, assertThrows } from "jsr:@std/assert@1";
import {
  buildActionCommand,
  type GlobalArgs,
  model,
  parseHomieList,
  publishCommand,
  readPropertyMetadata,
  readRetained,
  validatePayload,
  withClient,
} from "./valetudo.ts";

class FakeMqttClient extends EventEmitter {
  readonly publishCalls: Array<{
    topic: string;
    payload: string;
    options: unknown;
  }> = [];
  readonly endCalls: boolean[] = [];
  publishResult: Promise<unknown> = Promise.resolve(undefined);

  constructor(
    private readonly messages: Array<{
      topic: string;
      payload: string;
      delayMs?: number;
    }> = [],
    private readonly subscribeError?: Error,
  ) {
    super();
  }

  subscribeAsync(topics: string[]): Promise<unknown[]> {
    if (this.subscribeError) return Promise.reject(this.subscribeError);
    for (const message of this.messages) {
      if (topics.includes(message.topic)) {
        setTimeout(
          () =>
            this.emit(
              "message",
              message.topic,
              new TextEncoder().encode(message.payload),
            ),
          message.delayMs ?? 0,
        );
      }
    }
    return Promise.resolve([]);
  }

  publishAsync(
    topic: string,
    payload: string,
    options: unknown,
  ): Promise<unknown> {
    this.publishCalls.push({ topic, payload, options });
    return this.publishResult;
  }

  endAsync(force: boolean): Promise<void> {
    this.endCalls.push(force);
    return Promise.resolve();
  }

  asMqttClient(): MqttClient {
    return this as unknown as MqttClient;
  }
}

const GLOBAL_ARGS: GlobalArgs = {
  brokerUrl: "mqtt://example.test:1883",
  topicPrefix: "valetudo",
  identifier: "robot",
  connectTimeoutMs: 1000,
  readTimeoutMs: 500,
  publishTimeoutMs: 50,
};

function commandMetadataMessages(
  base: string,
  capability: string,
  property: string,
): Array<{ topic: string; payload: string; delayMs?: number }> {
  return [
    { topic: `${base}/$nodes`, payload: capability },
    { topic: `${base}/${capability}/$properties`, payload: property },
    { topic: `${base}/${capability}/${property}/$datatype`, payload: "enum" },
    { topic: `${base}/${capability}/${property}/$settable`, payload: "true" },
    {
      topic: `${base}/${capability}/${property}/$format`,
      payload: "START,STOP",
    },
  ];
}

Deno.test("parseHomieList normalizes retained Homie lists", () => {
  assertEquals(parseHomieList("one, two,,three"), ["one", "two", "three"]);
});

Deno.test("buildActionCommand maps basic controls", () => {
  assertEquals(
    buildActionCommand({ action: "home", iterations: 1 }),
    {
      capability: "BasicControlCapability",
      property: "operation",
      payload: "HOME",
    },
  );
});

Deno.test("buildActionCommand creates Valetudo segment-cleaning JSON", () => {
  assertEquals(
    buildActionCommand({
      action: "clean-segments",
      segmentIds: ["20", "18"],
      iterations: 2,
      customOrder: true,
    }),
    {
      capability: "MapSegmentationCapability",
      property: "clean",
      payload: '{"segment_ids":["20","18"],"iterations":2,"customOrder":true}',
    },
  );
});

Deno.test("buildActionCommand rejects incomplete and unsafe set-property actions", () => {
  assertThrows(
    () => buildActionCommand({ action: "set-property", iterations: 1 }),
    Error,
    "requires capability, property, and value",
  );
  assertThrows(
    () =>
      buildActionCommand({
        action: "set-property",
        capability: "Status/#",
        property: "value",
        value: "ON",
        iterations: 1,
      }),
    Error,
    "capability must contain only safe MQTT topic",
  );
});

Deno.test("global arguments reject wildcard topics", () => {
  const schema = model.globalArguments;
  assertEquals(
    schema.safeParse({
      brokerUrl: "mqtt://example.test:1883",
      topicPrefix: "valetudo/+",
      identifier: "robot",
    }).success,
    false,
  );
});

Deno.test("global arguments independently reject URL credentials", () => {
  assertEquals(
    model.globalArguments.safeParse({
      brokerUrl: "mqtt://user:secret@example.test:1883",
      topicPrefix: "valetudo",
      identifier: "robot",
    }).success,
    false,
  );
});

Deno.test("readRetained waits for every required topic and cleans up", async () => {
  const client = new FakeMqttClient([
    { topic: "robot/$settable", payload: "true", delayMs: 5 },
    { topic: "robot/$datatype", payload: "enum", delayMs: 200 },
  ]);
  const values = await readRetained(
    client.asMqttClient(),
    ["robot/$settable", "robot/$datatype"],
    500,
    ["robot/$settable", "robot/$datatype"],
  );
  assertEquals(values.get("robot/$datatype"), "enum");
  assertEquals(client.listenerCount("message"), 0);
});

Deno.test("readRetained rejects missing required topics and cleans up", async () => {
  const client = new FakeMqttClient([
    { topic: "robot/$settable", payload: "true" },
  ]);
  await assertRejects(
    () =>
      readRetained(
        client.asMqttClient(),
        ["robot/$settable", "robot/$datatype"],
        50,
        ["robot/$settable", "robot/$datatype"],
      ),
    Error,
    "robot/$datatype",
  );
  assertEquals(client.listenerCount("message"), 0);
});

Deno.test("readRetained reports subscribe failures and cleans up", async () => {
  const client = new FakeMqttClient([], new Error("denied"));
  await assertRejects(
    () => readRetained(client.asMqttClient(), ["robot/$state"], 50),
    Error,
    "MQTT subscribe failed: denied",
  );
  assertEquals(client.listenerCount("message"), 0);
});

Deno.test("readRetained cleans up after an optional-topic quiet period", async () => {
  const client = new FakeMqttClient([
    { topic: "robot/$state", payload: "ready" },
  ]);
  const values = await readRetained(
    client.asMqttClient(),
    ["robot/$state", "robot/$name"],
    500,
  );
  assertEquals(values.get("robot/$state"), "ready");
  assertEquals(client.listenerCount("message"), 0);
});

Deno.test("readPropertyMetadata requires current node and property membership", async () => {
  const base = "valetudo/robot";
  const capability = "FanSpeedControlCapability";
  const property = "preset";
  const client = new FakeMqttClient([
    { topic: `${base}/$nodes`, payload: capability },
    { topic: `${base}/${capability}/$properties`, payload: property },
    { topic: `${base}/${capability}/${property}/$datatype`, payload: "enum" },
    { topic: `${base}/${capability}/${property}/$settable`, payload: "true" },
    {
      topic: `${base}/${capability}/${property}/$format`,
      payload: "off,min,medium,max",
      delayMs: 50,
    },
  ]);
  assertEquals(
    await readPropertyMetadata(
      client.asMqttClient(),
      base,
      capability,
      property,
      500,
    ),
    {
      datatype: "enum",
      settable: true,
      format: "off,min,medium,max",
    },
  );
});

Deno.test("readPropertyMetadata rejects stale orphan settable metadata", async () => {
  const base = "valetudo/robot";
  const capability = "FanSpeedControlCapability";
  const property = "preset";
  const client = new FakeMqttClient([
    { topic: `${base}/$nodes`, payload: "BasicControlCapability" },
    { topic: `${base}/${capability}/$properties`, payload: property },
    { topic: `${base}/${capability}/${property}/$datatype`, payload: "enum" },
    { topic: `${base}/${capability}/${property}/$settable`, payload: "true" },
    { topic: `${base}/${capability}/${property}/$format`, payload: "off,max" },
  ]);
  await assertRejects(
    () =>
      readPropertyMetadata(
        client.asMqttClient(),
        base,
        capability,
        property,
        500,
      ),
    Error,
    "does not currently advertise FanSpeedControlCapability",
  );
});

Deno.test("readPropertyMetadata rejects stale property membership", async () => {
  const base = "valetudo/robot";
  const capability = "FanSpeedControlCapability";
  const property = "preset";
  const client = new FakeMqttClient([
    { topic: `${base}/$nodes`, payload: capability },
    { topic: `${base}/${capability}/$properties`, payload: "other" },
    { topic: `${base}/${capability}/${property}/$datatype`, payload: "enum" },
    { topic: `${base}/${capability}/${property}/$settable`, payload: "true" },
    { topic: `${base}/${capability}/${property}/$format`, payload: "off,max" },
  ]);
  await assertRejects(
    () =>
      readPropertyMetadata(
        client.asMqttClient(),
        base,
        capability,
        property,
        500,
      ),
    Error,
    `does not currently advertise ${capability}/${property}`,
  );
});

Deno.test("validatePayload fails closed for malformed metadata and values", () => {
  assertThrows(
    () =>
      validatePayload(
        { datatype: "enum", settable: true, format: null },
        "max",
      ),
    Error,
    "enum without a valid format",
  );
  assertThrows(
    () =>
      validatePayload(
        { datatype: "integer", settable: true, format: "0:100" },
        "1e2",
      ),
    Error,
    "must be an integer",
  );
  assertThrows(
    () =>
      validatePayload(
        { datatype: "integer", settable: true, format: "zero:100" },
        "50",
      ),
    Error,
    "invalid integer format",
  );
  assertThrows(
    () =>
      validatePayload(
        { datatype: "color", settable: true, format: null },
        "red",
      ),
    Error,
    "Unsupported Homie datatype",
  );
  assertThrows(
    () =>
      validatePayload(
        { datatype: "integer", settable: true, format: null },
        "9223372036854775808",
      ),
    Error,
    "signed 64-bit integer",
  );
  assertThrows(
    () =>
      validatePayload(
        { datatype: "integer", settable: true, format: null },
        "-9223372036854775809",
      ),
    Error,
    "signed 64-bit integer",
  );
  assertThrows(
    () =>
      validatePayload(
        {
          datatype: "integer",
          settable: true,
          format: "0:9223372036854775808",
        },
        "50",
      ),
    Error,
    "invalid integer range",
  );
  assertThrows(
    () =>
      validatePayload(
        { datatype: "integer", settable: true, format: "" },
        "50",
      ),
    Error,
    "invalid integer format",
  );
  assertThrows(
    () =>
      validatePayload(
        { datatype: "float", settable: true, format: "0.5:2.5" },
        "3.0",
      ),
    Error,
    "range 0.5:2.5",
  );
  assertThrows(
    () =>
      validatePayload(
        { datatype: "float", settable: true, format: "" },
        "1.5",
      ),
    Error,
    "invalid float format",
  );
});

Deno.test("publishCommand uses non-retained QoS 1", async () => {
  const base = "valetudo/robot";
  const capability = "BasicControlCapability";
  const property = "operation";
  const client = new FakeMqttClient(
    commandMetadataMessages(base, capability, property),
  );
  assertEquals(
    await publishCommand(
      client.asMqttClient(),
      base,
      { capability, property, payload: "START" },
      500,
      100,
    ),
    `${base}/${capability}/${property}/set`,
  );
  assertEquals(client.publishCalls, [{
    topic: `${base}/${capability}/${property}/set`,
    payload: "START",
    options: { qos: 1, retain: false },
  }]);
});

Deno.test("publish timeout still closes the MQTT client", async () => {
  const base = "valetudo/robot";
  const capability = "BasicControlCapability";
  const property = "operation";
  const client = new FakeMqttClient(
    commandMetadataMessages(base, capability, property),
  );
  client.publishResult = new Promise(() => undefined);
  await assertRejects(
    () =>
      withClient(
        GLOBAL_ARGS,
        (connected) =>
          publishCommand(
            connected,
            base,
            { capability, property, payload: "START" },
            500,
            25,
          ),
        () => Promise.resolve(client.asMqttClient()),
      ),
    Error,
    "MQTT publish",
  );
  assertEquals(client.endCalls, [true]);
});

Deno.test("withClient attempts one connection and propagates failure", async () => {
  let attempts = 0;
  await assertRejects(
    () =>
      withClient(
        GLOBAL_ARGS,
        async () => undefined,
        () => {
          attempts += 1;
          return Promise.reject(new Error("unreachable"));
        },
      ),
    Error,
    "unreachable",
  );
  assertEquals(attempts, 1);
});

Deno.test("withClient closes the client when an operation fails", async () => {
  const client = new FakeMqttClient();
  await assertRejects(
    () =>
      withClient(
        GLOBAL_ARGS,
        () => Promise.reject(new Error("operation failed")),
        () => Promise.resolve(client.asMqttClient()),
      ),
    Error,
    "operation failed",
  );
  assertEquals(client.endCalls, [true]);
});
