
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Storage.Blobs;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace LineCounter.Controllers
{
    [Route("/")]
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;
        private readonly BlobContainerClient _blobContainerClient;
        private readonly EventHubProducerClient _uploadsProducer;

        public HomeController(ILogger<HomeController> logger, IConfiguration configuration)
        {
            _logger = logger;
            _blobContainerClient = new BlobContainerClient(configuration["BlobConnectionString"], configuration["BlobContainer"]);
            _uploadsProducer = new EventHubProducerClient(configuration["EventHubsConnectionString"]);
        }

        public IActionResult Index()
        {
            return View();
        }

        [HttpPost("upload")]
        public async Task<IActionResult> Upload()
        {
            var names = new List<string>();

            foreach (var file in HttpContext.Request.Form.Files)
            {
                var fileName = file.FileName;

                await using var stream = file.OpenReadStream();

                _logger.LogInformation("Uploading {fileName}", fileName);

                await _blobContainerClient.UploadBlobAsync(fileName, stream);

                using EventDataBatch eventBatch = await _uploadsProducer.CreateBatchAsync();
                eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(fileName)));
                await _uploadsProducer.SendAsync(eventBatch);

                names.Add(fileName);
            }

            return View("Index", names.ToArray());
        }

        [HttpGet("status/{name}.html")]
        public async Task<string> Status(string name)
        {
            var properties = await _blobContainerClient.GetBlobClient(name).GetPropertiesAsync();
            properties.Value.Metadata.TryGetValue("whitespacecount", out var count);

            return count ?? "Unknown";
        }
    }
}