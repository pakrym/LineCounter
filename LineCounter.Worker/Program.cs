using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging;

namespace LineCounter.Worker
{
    public class Program
    {
        private static string EventHubConnectionString = "...";
        private static string BlobConnectionString = "...";
        private static string BlobContainerName = "...";

        private static CancellationToken _cancellationToken;
        private static ILogger<Program> _logger;
        private static BlobContainerClient _blobContainerClient;

        public static async Task Main()
        {
            // Setup a cancellation token that reacts to Ctrl+C
            var cancellationTokenSource = new CancellationTokenSource();
            Console.CancelKeyPress += (sender, eventArgs) =>
            {
                eventArgs.Cancel = true;
                cancellationTokenSource.Cancel();
            };

            _cancellationToken = cancellationTokenSource.Token;

            // Setup console logging
            _logger = LoggerFactory.Create(builder => builder.AddConsole()).
                CreateLogger<Program>();

            _logger.LogInformation("Starting processor host");

            _blobContainerClient = new BlobContainerClient(BlobConnectionString, BlobContainerName);

            var  processor = new EventProcessorClient(
                _blobContainerClient,
                EventHubConsumerClient.DefaultConsumerGroupName,
                EventHubConnectionString);

            processor.ProcessEventAsync += ProcessEvent;
            processor.ProcessErrorAsync += eventArgs =>
            {
                _logger.LogError(eventArgs.Exception, "Error in " + eventArgs.PartitionId);
                return Task.CompletedTask;
            };

            await processor.StartProcessingAsync();

            _logger.LogInformation("Processor host started");

            _cancellationToken.WaitHandle.WaitOne();

            _logger.LogInformation("Stopping processor host");

            await processor.StopProcessingAsync();
        }

        private static async Task ProcessEvent(ProcessEventArgs eventArgs)
        {
            if (eventArgs.Data != null)
            {
                var blobName = Encoding.UTF8.GetString(eventArgs.Data.Body.Span);

                var blobClient = _blobContainerClient.GetBlobClient(blobName);
                var downloadInfo = await blobClient.DownloadAsync(_cancellationToken);

                _logger.LogInformation("Processing {blob}", blobName);

                var fileStream = new MemoryStream();
                await downloadInfo.Value.Content.CopyToAsync(fileStream, _cancellationToken);

                var newLineCount = fileStream.ToArray().Count(b => b == '\n');

                await blobClient.SetMetadataAsync(
                    new Dictionary<string, string>()
                    {
                        { "whitespacecount", newLineCount.ToString()}
                    },
                    cancellationToken: _cancellationToken);

                _logger.LogInformation("{blob} had {lines} lines", blobName, newLineCount);

                await eventArgs.UpdateCheckpointAsync(_cancellationToken);
            }
        }
    }
}