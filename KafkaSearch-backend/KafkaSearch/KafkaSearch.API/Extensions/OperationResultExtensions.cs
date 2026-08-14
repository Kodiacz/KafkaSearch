namespace KafkaSearch.API.Extensions;

using KafkaSearch.Core.Common;
using Microsoft.AspNetCore.Mvc;

public static class OperationResultExtensions
{
    public static IActionResult ToActionResult<T>(
        this OperationResult<T> result, ControllerBase controller)
        => result.IsSuccess
            ? controller.Ok(result.Value)
            : result.Failure.ToActionResult(controller);

    public static IActionResult ToActionResult(
        this OperationResult result, ControllerBase controller)
        => result.IsSuccess
            ? controller.Ok(result)
            : result.Failure.ToActionResult(controller);

    public static IActionResult ToActionResult(
        this Failure failure, ControllerBase controller)
        => failure.StatusCode switch
        {
            400 => controller.BadRequest(failure.Message),
            404 => controller.NotFound(failure.Message),
            409 => controller.Conflict(failure.Message),
            _ => controller.StatusCode(failure.StatusCode, failure.Message)
        };
}