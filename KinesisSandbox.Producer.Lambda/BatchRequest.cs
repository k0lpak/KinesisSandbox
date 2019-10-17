using System;
using System.Collections.Generic;
using System.Text;

namespace KinesisSandbox.Producer.Lambda
{
    public class BatchRequest
    {
        public int BatchSize { get; set; }

        public int ItemSizeInKB { get; set; }
    }
}
