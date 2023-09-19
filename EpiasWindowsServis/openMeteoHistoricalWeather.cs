using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EpiasWindowsServis
{

    public class openMeteoHistoricalWeather
    {
        public float latitude { get; set; }
        public float longitude { get; set; }
        public float generationtime_ms { get; set; }
        public int utc_offset_seconds { get; set; }
        public string timezone { get; set; }
        public string timezone_abbreviation { get; set; }
        public float elevation { get; set; }
        public Hourly_Units hourly_units { get; set; }
        public Hourly hourly { get; set; }

        public class Hourly_Units
        {
            public string time { get; set; }
            public string relativehumidity_2m { get; set; }
            public string apparent_temperature { get; set; }
            public string windspeed_10m { get; set; }
        }

        public class Hourly
        {
            public DateTime[] time { get; set; }
            public int?[] relativehumidity_2m { get; set; }
            public float?[] apparent_temperature { get; set; }
            public float?[] windspeed_10m { get; set; }

        }

        //Bu WS için ekstra bir kod yazılmadı TR 39;35 koordinatına göre veriler çekildi.
        //Farklı veriler için geliştirilebilir. 
        public string getURL(DateTime startDate, DateTime endDate)
        {
            return @"https://archive-api.open-meteo.com/v1/archive?latitude=39&longitude=35&start_date=" + startDate.ToString("yyyy-MM-dd") + "&end_date=" + endDate.ToString("yyyy-MM-dd") + "&hourly=relativehumidity_2m,apparent_temperature,windspeed_10m&timezone=Europe%2FMoscow";
        }


    }
}
