using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.Lambda.Core;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace KinesisSandbox.Producer.Lambda
{
    public class Function
    {
        private readonly IAmazonKinesis _stream;
        private const string StreamName = "tripstream";
        private const int BatchSize = 50;
        private string[] _shards = new []
        {
             "0",
             "68056473384187692692674921486353642291",
             "136112946768375385385349842972707284582",
             "204169420152563078078024764459060926873",
             "272225893536750770770699685945414569164"
        };
        
        public Function()
        {
            _stream = new AmazonKinesisClient();
        }

        public async Task FunctionHandler(BatchRequest request, ILambdaContext context)
        {
            await SendBatchToStream(request);
        }

        private async Task SendBatchToStream(BatchRequest request)
        {
            var items = GenerateRecords(request);
            var skip = 0;
            for (var i = 0; i < items.Count; i += BatchSize)
            {
                var chunk = items.Skip(skip).Take(BatchSize).ToList();
                var batchRequest = new PutRecordsRequest
                {
                    StreamName = StreamName,
                    Records = GetStreamRecords(chunk)
                };
                var result = await _stream.PutRecordsAsync(batchRequest);

                if (result.FailedRecordCount > 0)
                {
                    LogInfo($"Failed Items: {result.FailedRecordCount}");
                }
            }

            LogInfo("Completed");
        }

        private IList<string> GenerateRecords(BatchRequest batch)
        {
            var rand = new Random();
            var records = new List<string>(batch.BatchSize);
            for (var i = 0; i < batch.BatchSize; i++)
            {
                records.Add(new string('1', rand.Next(10, 50) /*batch.ItemSizeInKB*/ * 1000 / 2));
            }

            return records;
        }

        private List<PutRecordsRequestEntry> GetStreamRecords(IList<string> records)
        {
            return records.Select((record, index) => new PutRecordsRequestEntry
            {
                PartitionKey = $"pk-{index}",
                ExplicitHashKey = $"{_shards[index % _shards.Length]}",
                Data = new MemoryStream(Encoding.UTF8.GetBytes(record))
            }).ToList();
        }

        private static void LogInfo(string message)
        {
            Console.WriteLine($"INFO: {message}");
        }
    }
}
