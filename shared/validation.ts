const PEER_ID_REGEX = /^[a-z0-9]{8}$/;
const SCOPE_VALUES = new Set(["machine", "directory", "repo"]);

export const MAX_PATH_CHARS = 4096;
export const MAX_TTY_CHARS = 64;
export const MAX_SUMMARY_CHARS = 280;
export const MAX_MESSAGE_CHARS = 2000;

export class ValidationError extends Error {
  readonly status: number;

  constructor(message: string, status = 400) {
    super(message);
    this.status = status;
  }
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function isValidPeerId(value: unknown): value is string {
  return typeof value === "string" && PEER_ID_REGEX.test(value);
}

export function requirePeerId(value: unknown, field: string): string {
  if (!isValidPeerId(value)) {
    throw new ValidationError(`${field} must be an 8-character lowercase alphanumeric peer ID`);
  }
  return value;
}

export function requireScope(value: unknown): "machine" | "directory" | "repo" {
  if (typeof value !== "string" || !SCOPE_VALUES.has(value)) {
    throw new ValidationError('scope must be one of: "machine", "directory", "repo"');
  }
  return value as "machine" | "directory" | "repo";
}

export function requirePositiveInt(value: unknown, field: string): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value <= 0) {
    throw new ValidationError(`${field} must be a positive integer`);
  }
  return value;
}

export function requireString(
  value: unknown,
  field: string,
  opts: { max: number; min?: number; allowEmpty?: boolean }
): string {
  if (typeof value !== "string") {
    throw new ValidationError(`${field} must be a string`);
  }
  if ((opts.min ?? (opts.allowEmpty ? 0 : 1)) > value.length) {
    throw new ValidationError(`${field} must be at least ${opts.min ?? 1} characters`);
  }
  if (value.length > opts.max) {
    throw new ValidationError(`${field} must be at most ${opts.max} characters`);
  }
  return value;
}

export function requireOptionalString(
  value: unknown,
  field: string,
  opts: { max: number; allowEmpty?: boolean } = { max: MAX_PATH_CHARS }
): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  return requireString(value, field, { max: opts.max, allowEmpty: opts.allowEmpty ?? true });
}

export function parseRequiredToken(value: string | undefined | null): string {
  const token = (value ?? "").trim();
  if (!token) {
    throw new Error("CLAUDE_PEERS_TOKEN is required and must be non-empty");
  }
  return token;
}

export function parsePositiveIntEnv(
  value: string | undefined,
  fallback: number,
  field: string
): number {
  if (!value) {
    return fallback;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${field} must be a positive integer`);
  }
  return parsed;
}
