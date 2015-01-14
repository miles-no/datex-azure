using System;
using System.Collections.Generic;

namespace Vegvesen.Web.Models
{
    public class ServiceData
    {
        public string ServiceName { get; set; }
        public DateTimeOffset? LastModified { get; set; }
        public IEnumerable<string> DataCollection { get; set; } 
    }
}