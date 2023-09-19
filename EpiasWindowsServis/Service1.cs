using EpiasModels.Consumption;
using EpiasModels.Market;
using EpiasModels.Production;
using GDataHub;
using GDataHub.Utils;
using Newtonsoft.Json;
using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Net;
using System.ServiceProcess;
using System.Timers;

namespace EpiasWindowsServis
{
    public partial class Service1 : ServiceBase
    {       
        Timer timer = new Timer();

        public Service1()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            WriteToFile(DateTime.Now.ToString() + " : " + "Windows Servis Başlatıldı...");
            timer.Elapsed += new ElapsedEventHandler(OnElapsedTime);
            timer.Interval = 3600000; //ms olarak ayarlanmakta - 3600000ms =1 saat - 21600000ms=6 saat
            timer.Enabled = true;
        }

        protected override void OnStop()
        {
            WriteToFile(DateTime.Now.ToString() + " : " + "Windows Servis Durduruldu...");
        }

        //Timer'ın Her Clock Cycle'ında çalışacak Kodumuz
        private void OnElapsedTime(object source, ElapsedEventArgs e)
        {
            DateTime dtToday = DateTime.Now;

            //DataBase hazırlık
            string strDbConnectionString = ConfigurationManager.ConnectionStrings["dbCon"].ConnectionString.ToString();
            IDataBase db = DataBaseSwitcher.GetDataBase(Util.DBType.SQL, strDbConnectionString);

            try
            {
                WriteToFile(DateTime.Now.ToString() + " : " + "Web Servis Aktarımı Başladı...");
                // PTF değerlerinin WS'den çekilmesi ve DB import
                mcpValuesImportDb(dtToday, db);

                // Yük Tahmini değerlerinin WS'den çekilmesi ve DB import           
                lepValuesImportDb(dtToday, db);

                //KGÜP total değerlerinin WS'den çekilmesi ve DB import
                dppValuesImportDb(dtToday, db);

                //İşlem Hacmi değerlerinin WS'den çekilmesi ve DB import
                tradeVolumeValuesImportDb(dtToday, db);

                // Eşleşme Miktarı değerlerinin WS'den çekilmesi ve DB import
                matchedQuantityValuesImportDb(dtToday, db);

                //Open-Meteo Web Servisinden Hava Verilerinin çekilmesi ve DB import
                openMeteoWeatherValuesImportDb(dtToday, db);

                WriteToFile(DateTime.Now.ToString() + " : " + "Web Servis Aktarımı Tamamlandı...");
            }
            catch (Exception ex)
            {
                WriteToFile(DateTime.Now.ToString() + " : " + "__HATA OLUŞTU__ ");
                WriteToFile(ex.Message.ToString() + " \r\n");
            }
        }

        public void WriteToFile(string Message)
        {
            string path = AppDomain.CurrentDomain.BaseDirectory + "\\Logs";
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
            string filepath = AppDomain.CurrentDomain.BaseDirectory + "\\Logs\\ServiceLog_" + DateTime.Now.Date.ToShortDateString().Replace('/', '_') + ".txt";
            if (!File.Exists(filepath))
            {
                // Dosya yoksa oluşturuyoruz.
                using (StreamWriter sw = File.CreateText(filepath))
                {
                    sw.WriteLine(Message);
                }
            }
            else
            {//varsa sonuna ekleme yapıyoruz.
                using (StreamWriter sw = File.AppendText(filepath))
                {
                    sw.WriteLine(Message);
                }
            }
        }

        private void openMeteoWeatherValuesImportDb(DateTime dtToday, IDataBase db)
        {
            WebClient client = new WebClient();
            string txtWebService = "", urlWebService;
            string strUpdateSql = "UPDATE dayAheadMarketTR  SET TEMPERATURE = @TEMPERATURE, HUMIDITY = @HUMIDITY, WINDSPEED = @WINDSPEED WHERE TARIH_INT = @TARIH_INT AND SAAT = @SAAT";
            int counter = 0;
            DateTime startDate = setStartDate(db, "TEMPERATURE");
            startDate = startDate <= Convert.ToDateTime("01.11.2011") ? Convert.ToDateTime("01.11.2011") : startDate.AddDays(-1);
            DateTime endDate;
            IDataParameter[] weatherParameters = new IDataParameter[5];

            //Tarih kontrolü
            while (startDate < dtToday)
            {
                // fark 1 yıldan fazla ise 1 yıllık veri çek değilse hepsini çek.
                endDate = setEndDate(dtToday, startDate);

                // WS'den veriyi alıp DB'e yaz 
                openMeteoHistoricalWeather weatherData = new openMeteoHistoricalWeather();
                urlWebService = weatherData.getURL(startDate, endDate);
                txtWebService = client.DownloadString(urlWebService);
                weatherData = JsonConvert.DeserializeObject<openMeteoHistoricalWeather>(txtWebService);

                for (int i = 0; i < weatherData.hourly.apparent_temperature.Length; i++)
                {
                    if (weatherData.hourly.apparent_temperature[i] != null)
                    {
                        weatherParameters[0] = new SqlParameter("@TEMPERATURE", weatherData.hourly.apparent_temperature[i]);
                        weatherParameters[1] = new SqlParameter("@HUMIDITY", weatherData.hourly.relativehumidity_2m[i]);
                        weatherParameters[2] = new SqlParameter("@WINDSPEED", weatherData.hourly.windspeed_10m[i]);
                        weatherParameters[3] = new SqlParameter("@TARIH_INT", Convert.ToInt32(weatherData.hourly.time[i].ToString("yyyyMMdd")));
                        weatherParameters[4] = new SqlParameter("@SAAT", weatherData.hourly.time[i].Hour);
                        db.ExecuteNonQuery(strUpdateSql, weatherParameters);
                        counter++;
                    }
                }
                startDate = startDate.AddYears(1);
            }
            WriteToFile(DateTime.Now.ToString() + " : " + counter.ToString() + " Satır Sıcaklık, Nem ve Rüzgar Verisi Open-Meteo Web Servisi Üzerinden Aktarımı Tamamlandı.");
        }

        private void matchedQuantityValuesImportDb(DateTime dtToday, IDataBase db)
        {
            WebClient client = new WebClient();
            string txtWebService = "", urlWebService;
            string strUpdateSql = "UPDATE dayAheadMarketTR  SET MATCHEDQUANTITY = @MATCHEDQUANTITY WHERE TARIH_INT = @TARIH_INT AND SAAT = @SAAT";
            int counter = 0;
            DateTime startDate = setStartDate(db, "MATCHEDQUANTITY");
            DateTime endDate;
            IDataParameter[] mqParameters = new IDataParameter[3];

            //Tarih kontrolü
            while (startDate < dtToday)
            {
                // fark 1 yıldan fazla ise 1 yıllık veri çek değilse hepsini çek.
                endDate = setEndDate(dtToday, startDate);

                // WS'den veriyi alıp DB'e yaz 
                dayAheadMarketVolumeResponse matchedQuantity = new dayAheadMarketVolumeResponse();
                urlWebService = matchedQuantity.getURL(startDate, endDate, null, null);
                txtWebService = client.DownloadString(urlWebService);
                matchedQuantity = JsonConvert.DeserializeObject<dayAheadMarketVolumeResponse>(txtWebService);

                if (matchedQuantity.resultCode == "0")
                {
                    foreach (var item in matchedQuantity.body.dayAheadMarketVolumeList)
                    {
                        mqParameters[0] = new SqlParameter("@MATCHEDQUANTITY", item.matchedBids);
                        mqParameters[1] = new SqlParameter("@TARIH_INT", Convert.ToInt32(item.date.ToString("yyyyMMdd")));
                        mqParameters[2] = new SqlParameter("@SAAT", item.date.Hour);

                        db.ExecuteNonQuery(strUpdateSql, mqParameters);
                        counter++;
                    }
                }
                else
                {
                    WriteToFile("Web Servis Dönüş Hatası : Matched Quantity " + DateTime.Now.ToString());
                }
                startDate = startDate.AddYears(1);
            }
            WriteToFile(DateTime.Now.ToString() + " : " + counter.ToString() + " Satır Eşleşme Miktarı (matchedQuantity) Verisi Web Servis Aktarımı Tamamlandı.");
        }

        private void tradeVolumeValuesImportDb(DateTime dtToday, IDataBase db)
        {
            WebClient client = new WebClient();
            string txtWebService = "", urlWebService;
            string strUpdateSql = "UPDATE dayAheadMarketTR  SET TRADEVOLUME = @TRADEVOLUME WHERE TARIH_INT = @TARIH_INT AND SAAT = @SAAT";
            int counter = 0;
            DateTime startDate = setStartDate(db, "TRADEVOLUME");
            DateTime endDate;
            IDataParameter[] tvParameters = new IDataParameter[3];

            //Tarih kontrolü
            while (startDate < dtToday)
            {
                // fark 1 yıldan fazla ise 1 yıllık veri çek değilse hepsini çek.
                endDate = setEndDate(dtToday, startDate);

                // WS'den veriyi alıp DB'e yaz 
                dayAheadMarketTradeVolumeResponse tradeVolume = new dayAheadMarketTradeVolumeResponse();
                urlWebService = tradeVolume.getURL(startDate, endDate);
                txtWebService = client.DownloadString(urlWebService);
                tradeVolume = JsonConvert.DeserializeObject<dayAheadMarketTradeVolumeResponse>(txtWebService);

                if (tradeVolume.resultCode == "0")
                {
                    foreach (var item in tradeVolume.body.dayAheadMarketTradeVolumeList)
                    {
                        tvParameters[0] = new SqlParameter("@TRADEVOLUME", item.volumeOfAsk);
                        tvParameters[1] = new SqlParameter("@TARIH_INT", Convert.ToInt32(item.date.ToString("yyyyMMdd")));
                        tvParameters[2] = new SqlParameter("@SAAT", item.date.Hour);

                        db.ExecuteNonQuery(strUpdateSql, tvParameters);
                        counter++;
                    }
                }
                else
                {
                    WriteToFile("Web Servis Dönüş Hatası : Trade Volume " + DateTime.Now.ToString());
                }
                startDate = startDate.AddYears(1);
            }

            WriteToFile(DateTime.Now.ToString() + " : " + counter.ToString() + " Satır İşlem Hacmi (tradeVolume) Verisi Web Servis Aktarımı Tamamlandı.");
        }

        private void dppValuesImportDb(DateTime dtToday, IDataBase db)
        {
            WebClient client = new WebClient();
            string txtWebService = "", urlWebService;
            string strUpdateSql = "UPDATE dayAheadMarketTR  SET DPP = @DPP WHERE TARIH_INT = @TARIH_INT AND SAAT = @SAAT";
            int counter = 0;
            DateTime startDate = setStartDate(db, "DPP");
            DateTime endDate;
            IDataParameter[] dppParameters = new IDataParameter[3];

            //Tarih kontrolü
            while (startDate < dtToday)
            {
                // fark 1 yıldan fazla ise 1 yıllık veri çek değilse hepsini çek.
                endDate = setEndDate(dtToday, startDate);

                // WS'den veriyi alıp DB'e yaz 
                finalDPPResponse DPP = new finalDPPResponse();
                urlWebService = DPP.getURL(startDate, endDate);
                txtWebService = client.DownloadString(urlWebService);
                DPP = JsonConvert.DeserializeObject<finalDPPResponse>(txtWebService);


                if (DPP.resultCode == "0")
                {
                    foreach (var item in DPP.body.finalDPPList)
                    {
                        dppParameters[0] = new SqlParameter("@DPP", item.dpp);
                        dppParameters[1] = new SqlParameter("@TARIH_INT", Convert.ToInt32(item.date.ToString("yyyyMMdd")));
                        dppParameters[2] = new SqlParameter("@SAAT", item.date.Hour);

                        db.ExecuteNonQuery(strUpdateSql, dppParameters);
                        counter++;
                    }
                }
                else
                {
                    WriteToFile("Web Servis Dönüş Hatası : DPP " + DateTime.Now.ToString());
                }

                startDate = startDate.AddYears(1);
            }
            WriteToFile(DateTime.Now.ToString() + " : " + counter.ToString() + " Satır Günlük Üretim Planı (DPP) Verisi Web Servis Aktarımı Tamamlandı.");
        }

        private void lepValuesImportDb(DateTime dtToday, IDataBase db)
        {
            WebClient client = new WebClient();
            string txtWebService = "", urlWebService;
            string strUpdateSql = "UPDATE dayAheadMarketTR  SET LEP = @LEP WHERE TARIH_INT = @TARIH_INT AND SAAT = @SAAT";
            int counter = 0;
            DateTime startDate = setStartDate(db, "LEP");
            DateTime endDate;
            IDataParameter[] lepParameters = new IDataParameter[3];

            //Tarih kontrolü
            while (startDate < dtToday)
            {
                // fark 1 yıldan fazla ise 1 yıllık veri çek değilse hepsini çek.
                endDate = setEndDate(dtToday, startDate);

                // WS'den veriyi alıp DB'e yaz 
                loadEstimationPlanResponse LEP = new loadEstimationPlanResponse();
                urlWebService = LEP.getURL(startDate, endDate);
                txtWebService = client.DownloadString(urlWebService);
                LEP = JsonConvert.DeserializeObject<loadEstimationPlanResponse>(txtWebService);

                if (LEP.resultCode == "0")
                {
                    foreach (var item in LEP.body.loadEstimationPlanList)
                    {
                        lepParameters[0] = new SqlParameter("@LEP", item.lep);
                        lepParameters[1] = new SqlParameter("@TARIH_INT", Convert.ToInt32(item.date.ToString("yyyyMMdd")));
                        lepParameters[2] = new SqlParameter("@SAAT", item.date.Hour);

                        db.ExecuteNonQuery(strUpdateSql, lepParameters);
                        counter++;
                    }
                }
                else
                {
                    WriteToFile("Web Servis Dönüş Hatası : LEP " + DateTime.Now.ToString());
                }
                startDate = startDate.AddYears(1);
            }
            WriteToFile(DateTime.Now.ToString() + " : " + counter.ToString() + " Satır Yük Tahmini (LEP) Verisi Web Servis Aktarımı Tamamlandı. ");
        }

        private void mcpValuesImportDb(DateTime dtToday, IDataBase db)
        {
            WebClient client = new WebClient();
            string txtWebService = "", urlWebService;
            string strInsertSql = "INSERT dayAheadMarketTR (TARIH, TARIH_INT, SAAT, WEEKDAY, PTF_TR, PTF_EUR, PTF_USD ) VALUES (@TARIH, @TARIH_INT, @SAAT, @WEEKDAY, @PTF_TR, @PTF_EUR, @PTF_USD)";
            int counter = 0;
            DateTime startDate = setStartDate(db, "PTF_TR");
            DateTime endDate;
            IDataParameter[] mcpParameters = new IDataParameter[7];

            // Tarih kontrolü 
            while (startDate < dtToday)
            {
                // fark 1 yıldan fazla ise 1 yıllık veri çek değilse hepsini çek.
                endDate = setEndDate(dtToday, startDate);

                // WS'den veriyi alıp DB'e yaz 
                dayAheadMCPResponse MCP = new dayAheadMCPResponse();
                urlWebService = MCP.getURL(startDate, endDate);
                txtWebService = client.DownloadString(urlWebService);
                MCP = JsonConvert.DeserializeObject<dayAheadMCPResponse>(txtWebService);

                if (MCP.resultCode == "0")
                {
                    foreach (var item in MCP.body.dayAheadMCPList)
                    {
                        mcpParameters[0] = new SqlParameter("@TARIH", item.date);
                        mcpParameters[1] = new SqlParameter("@TARIH_INT", Convert.ToInt32(item.date.ToString("yyyyMMdd")));
                        mcpParameters[2] = new SqlParameter("@SAAT", item.date.Hour);
                        mcpParameters[3] = new SqlParameter("@WEEKDAY", ((int)item.date.DayOfWeek == 0) ? 7 : (int)item.date.DayOfWeek);
                        mcpParameters[4] = new SqlParameter("@PTF_TR", item.price);
                        mcpParameters[5] = new SqlParameter("@PTF_EUR", item.priceEur);
                        mcpParameters[6] = new SqlParameter("@PTF_USD", item.priceUsd);

                        db.ExecuteNonQuery(strInsertSql, mcpParameters);
                        counter++;
                    }
                }
                else
                {
                    WriteToFile("Web Servis Dönüş Hatası : MCP " + DateTime.Now.ToString());
                }
                startDate = startDate.AddYears(1);
            }
            WriteToFile(DateTime.Now.ToString() + " : " + counter.ToString() + " Satır PTF (MCP) Verisi Web Servis Aktarımı Tamamlandı.");
        }

        private DateTime setEndDate(DateTime dtToday, DateTime startDate)
        {
            DateTime endDate = (dtToday.Year - startDate.Year) >= 1 ? startDate.AddYears(1).AddDays(-1) : DateTime.Now;
            endDate = endDate > dtToday ? dtToday : endDate;
            return endDate;
        }

        private DateTime setStartDate(IDataBase db, string fieldName)
        {
            DateTime dtMinDate = Convert.ToDateTime("01.11.2011");
            string strSql = "SELECT MAX(TARIH) AS SONTARIH FROM DAYAHEADMARKETTR  WHERE " + fieldName + " IS NOT NULL";
            DataTable dtTarih = db.ExecuteQueryDataTable(strSql);
            DateTime startDate;

            if (dtTarih.Rows.Count > 0 && dtTarih.Rows[0][0] != DBNull.Value)
            {
                //tarih varsa bu tarihin 1 gün sonrasından işlem yapılır. 
                startDate = Convert.ToDateTime(dtTarih.Rows[0][0]).AddDays(1);
            }
            else
            {
                // tarih yoksa min. tarih ile işlem yapılır.
                startDate = dtMinDate;
            }
            return startDate;
        }



    }
}
