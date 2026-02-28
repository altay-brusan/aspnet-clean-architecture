using Evently.Common.Domain;
using MediatR;
using Microsoft.Extensions.Logging;
using Serilog.Context;

namespace Evently.Common.Application.Behaviors;

internal sealed class RequestLoggingPipelineBehavior<TRequest, TResponse>(
    ILogger<RequestLoggingPipelineBehavior<TRequest, TResponse>> logger)
    : IPipelineBehavior<TRequest, TResponse>
    where TRequest : class
    where TResponse : Result
{
    public async Task<TResponse> Handle(
        TRequest request,
        RequestHandlerDelegate<TResponse> next,
        CancellationToken cancellationToken)
    {
        string moduleName = GetModuleName(typeof(TRequest).FullName!);
        string requestName = typeof(TRequest).Name;

        using (LogContext.PushProperty("Module", moduleName))
        {
#pragma warning disable CA1873 // Avoid potentially expensive logging
            logger.LogInformation("Processing request {RequestName}", requestName);
#pragma warning restore CA1873 // Avoid potentially expensive logging

            TResponse result = await next();

            if (result.IsSuccess)
            {
#pragma warning disable CA1873 // Avoid potentially expensive logging
                logger.LogInformation("Completed request {RequestName}", requestName);
#pragma warning restore CA1873 // Avoid potentially expensive logging
            }
            else
            {
                using (LogContext.PushProperty("Error", result.Error, true))
                {
                    logger.LogError("Completed request {RequestName} with error", requestName);
                }
            }

            return result;
        }
    }

    private static string GetModuleName(string requestName) => requestName.Split('.')[2];
}
