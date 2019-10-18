using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
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
        private List<string> _shards;
        
        public Function()
        {
            _stream = new AmazonKinesisClient();
        }

        public async Task FunctionHandler(BatchRequest request, ILambdaContext context)
        {
            await ComputeShards();
            await SendBatchToStream(request);
        }

        private async Task ComputeShards()
        {
            var streamConfig = await _stream.DescribeStreamAsync(new DescribeStreamRequest
            {
                StreamName = StreamName
            });
            _shards = streamConfig.StreamDescription.Shards
                .Where(shard => string.IsNullOrEmpty(shard.SequenceNumberRange.EndingSequenceNumber))
                .Select(shard => shard.HashKeyRange.StartingHashKey).ToList();
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

            LogInfo($"Completed. {DateTime.UtcNow}");
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
                PartitionKey = Guid.NewGuid().ToString(), //$"pk-{index}",
                //ExplicitHashKey = $"{_shards[index % _shards.Count]}",
                Data = new MemoryStream(Encoding.UTF8.GetBytes(record))
            }).ToList();
        }

        private static void LogInfo(string message)
        {
            Console.WriteLine($"INFO: {message}");
        }
    }
}
