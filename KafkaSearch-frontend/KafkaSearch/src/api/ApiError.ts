// api/ApiError.ts
export type ValidationProblem = {
  title?: string;
  status: number;
  errors?: Record<string, string[]>;
  traceId?: string;
};

export class ApiError extends Error {
  status: number;
  problem: ValidationProblem | null;
  rawBody: string;

  constructor(status: number, problem: ValidationProblem | null, rawBody: string) {
    super(problem?.title ?? `Request failed (${status})`);
    this.status = status;
    this.problem = problem;
    this.rawBody = rawBody;
  }

  get messages(): string[] {
    if (!this.problem?.errors) return [this.message];
    return Object.entries(this.problem.errors).flatMap(([field, msgs]) =>
      msgs.map((m) => `${field}: ${m}`)
    );
  }
}