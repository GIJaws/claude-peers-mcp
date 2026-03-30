import { describe, expect, test } from "bun:test";
import {
  MAX_MESSAGE_CHARS,
  ValidationError,
  isValidPeerId,
  parsePositiveIntEnv,
  parseRequiredToken,
  requirePeerId,
  requireScope,
  requireString,
} from "./validation.ts";

describe("validation", () => {
  test("accepts valid peer IDs", () => {
    expect(isValidPeerId("abc123de")).toBe(true);
    expect(() => requirePeerId("abc123de", "id")).not.toThrow();
  });

  test("rejects invalid peer IDs", () => {
    expect(isValidPeerId("ABC123DE")).toBe(false);
    expect(() => requirePeerId("too-long-id", "id")).toThrow(ValidationError);
  });

  test("validates scope", () => {
    expect(requireScope("machine")).toBe("machine");
    expect(() => requireScope("global")).toThrow(ValidationError);
  });

  test("validates bounded strings", () => {
    expect(requireString("ok", "summary", { max: 10, allowEmpty: true })).toBe("ok");
    expect(() =>
      requireString("x".repeat(MAX_MESSAGE_CHARS + 1), "text", { max: MAX_MESSAGE_CHARS })
    ).toThrow(ValidationError);
  });

  test("requires non-empty token", () => {
    expect(parseRequiredToken(" abc ")).toBe("abc");
    expect(() => parseRequiredToken("")).toThrow("CLAUDE_PEERS_TOKEN is required");
  });

  test("parses integer env values", () => {
    expect(parsePositiveIntEnv(undefined, 5, "X")).toBe(5);
    expect(parsePositiveIntEnv("10", 5, "X")).toBe(10);
    expect(() => parsePositiveIntEnv("0", 5, "X")).toThrow("X must be a positive integer");
  });
});
