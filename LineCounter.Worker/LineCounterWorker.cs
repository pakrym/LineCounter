using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace LineCounter
{
    public class LineCounterWorker: IHostedService
    {
        private readonly IConfiguration _configuration;
        private static ILogger<LineCounterWorker> _logger;
        private static BlobContainerClient _blobContainerClient;
        private EventProcessorClient _processor;

        public LineCounterWorker(IConfiguration configuration, ILogger<LineCounterWorker> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            _blobContainerClient = new BlobContainerClient(_configuration["BlobConnectionString"], _configuration["BlobContainer"]);

            _processor = new EventProcessorClient(
                _blobContainerClient,
                EventHubConsumerClient.DefaultConsumerGroupName,
                _configuration["EventHubsConnectionString"]);

            _processor.ProcessEventAsync += ProcessEvent;
            _processor.ProcessErrorAsync += eventArgs =>
            {
                _logger.LogError(eventArgs.Exception, "Error in " + eventArgs.PartitionId);
                return Task.CompletedTask;
            };


            _logger.LogInformation("Starting processor host");

            await _processor.StartProcessingAsync(cancellationToken);
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            await _processor.StopProcessingAsync(cancellationToken);
        }

        private static async Task ProcessEvent(ProcessEventArgs eventArgs)
        {
            if (eventArgs.Data != null)
            {
                try
                {
                    var blobName = Encoding.UTF8.GetString(eventArgs.Data.Body.Span);

                    var blobClient = _blobContainerClient.GetBlobClient(blobName);
                    var downloadInfo = await blobClient.DownloadAsync(eventArgs.CancellationToken);

                    _logger.LogInformation("Processing {blob}", blobName);

                    var fileStream = new MemoryStream();
                    await downloadInfo.Value.Content.CopyToAsync(fileStream, eventArgs.CancellationToken);

                    var newLineCount = fileStream.ToArray().Count(b => b == '\n');

                    await blobClient.SetMetadataAsync(
                        new Dictionary<string, string>
                        {
                            { "whitespacecount", newLineCount.ToString()}
                        },
                        cancellationToken: eventArgs.CancellationToken);

                    _logger.LogInformation("{blob} had {lines} lines", blobName, newLineCount);
                }
                finally
                {
                    await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
                }
            }
        }
    }
}