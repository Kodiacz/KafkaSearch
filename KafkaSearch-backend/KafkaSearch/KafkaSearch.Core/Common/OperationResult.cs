namespace KafkaSearch.Core.Common;

using System.Runtime.CompilerServices;

public enum FailureType : byte
{
	None,
	Operation,
	Validation
}

public readonly record struct Failure
{
	private static readonly int DefaultValidatioFailureErrorCode = 400;
	private static readonly int DefaultOperationFailureErrorCode = 500;

	public static readonly Failure NoFailure = new(default, default, default, default, default);

	private Failure(Exception? exception, string? message, int code, FailureType type, string? operationName)
	{
		Exception = exception;
		Message = message;
		StatusCode = code;
		Type = type;
		OperationName = operationName;
	}

	public Exception? Exception { get; }
	public string? Message { get; }
	public int StatusCode { get; }
	public FailureType Type { get; }
	public bool IsValidation => Type == FailureType.Validation;
	public string? OperationName { get; }

	public static Failure Validation(string message, [CallerMemberName] string? operationName = null)
		=> new(default, message, DefaultValidatioFailureErrorCode, FailureType.Validation, operationName: operationName);

	public static Failure Validation(string message, int code, [CallerMemberName] string? operationName = null)
		=> new(default, message, code, FailureType.Validation, operationName: operationName);

	public static Failure Operation(string message, [CallerMemberName] string? operationName = null)
		=> new(default, message, DefaultOperationFailureErrorCode, FailureType.Operation, operationName: operationName);

	public static Failure Operation(string message, int code, [CallerMemberName] string? operationName = null)
		=> new(default, message, code, FailureType.Operation, operationName: operationName);

	public static Failure Operation(Exception exception, [CallerMemberName] string? operationName = null)
		=> new(exception, exception.InnerException?.Message ?? exception?.Message, DefaultOperationFailureErrorCode, FailureType.Operation, operationName: operationName);

	public static Failure Operation(Exception exception, string message, [CallerMemberName] string? operationName = null)
		=> new(exception, message, DefaultOperationFailureErrorCode, FailureType.Operation, operationName: operationName);

	public static Failure Operation(Exception exception, string message, int code, [CallerMemberName] string? operationName = null)
		=> new(exception, message, code, FailureType.Operation, operationName: operationName);

	public static Failure Merge(params Failure[] failures)
	{
		if (failures is null) throw new ArgumentNullException(nameof(failures));
		if (failures.Length == 0) throw new ArgumentOutOfRangeException(nameof(failures));

		var message = string.Join(';', failures
			.Where(f => !string.IsNullOrWhiteSpace(f.Message))
			.Select(f => f.Message)
			.Distinct()
			.ToArray());

		if (failures.Any(f => f.Exception is not null))
			return Operation(new AggregateException(failures
				.Where(f => f.Exception is not null)
				.Select(f => f.Exception!)
				.ToArray()), message);

		if (failures.Any(f => !f.IsValidation))
			return Operation(message);

		return Validation(message);
	}

	public static implicit operator Failure(Exception exception)
		=> Operation(exception);
}

public readonly record struct OperationResult
{
	public Failure Failure { get; }

	public bool IsSuccess => !IsFailure;

	public bool IsFailure => Failure != Failure.NoFailure;

	private OperationResult(Failure failure)
		=> Failure = failure;

	public static OperationResult Ok() =>
		new(Failure.NoFailure);

	public static OperationResult<TValue> Ok<TValue>(TValue value) =>
		new(value, Failure.NoFailure);

	public static OperationResult Fail(Exception exception) =>
		new(exception);

	public static OperationResult Fail(Failure failure) =>
		new(failure);

	public static OperationResult<TValue> Fail<TValue>(Failure failure) =>
		new(default, failure);

	public static OperationResult<TValue?> Fail<TValue>(Exception exception) =>
		new(default, exception);

	public static OperationResult<TValue> Try<TValue>(Func<TValue> operation)
	{
		try { return Ok(operation()); }
		catch (Exception ex) { return Fail(ex); }
	}

	public static async Task<OperationResult<TValue>> TryAsync<TValue>(Func<Task<TValue>> operation)
	{
		try { return Ok(await operation().ConfigureAwait(false)); }
		catch (Exception ex) { return Fail(ex); }
	}

	public static async Task<OperationResult> TryAsync(Func<Task> operation)
	{
		try { await operation().ConfigureAwait(false); return Ok(); }
		catch (Exception ex) { return Fail(ex); }
	}
}

public readonly record struct OperationResult<TValue>
{
	public Failure Failure { get; }
	public bool IsSuccess => !IsFailure;
	public bool IsFailure => Failure != Failure.NoFailure;
	public TValue? Value { get; }

	internal OperationResult(TValue? value, Failure failure)
	{
		Value = value;
		Failure = failure;
	}

	public static implicit operator OperationResult(OperationResult<TValue> operationResult) =>
		operationResult.IsSuccess ?
		OperationResult.Ok() :
		OperationResult.Fail(operationResult.Failure);

	public static implicit operator OperationResult<TValue>(OperationResult operationResult) =>
		operationResult.IsSuccess ?
		OperationResult.Ok<TValue>(default!) :
		OperationResult.Fail<TValue>(operationResult.Failure);
}

public static class OperationResultExtensions
{
	public static Task<OperationResult> AsCompletedTask(this OperationResult operationResult)
		=> Task.FromResult(operationResult);

	public static Task<OperationResult<TValue>> AsCompletedTask<TValue>(this OperationResult<TValue> operationResult)
		=> Task.FromResult(operationResult);
}
