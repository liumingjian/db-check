export type ApiErrorCode = "unauthorized" | "forbidden" | "not_found" | "invalid" | "failed";

/** The one error type every contract operation rejects with. */
export class ApiError extends Error {
  constructor(
    readonly code: ApiErrorCode,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}
