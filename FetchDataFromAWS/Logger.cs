using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Net.Mail;
using System.Text;

namespace FetchDataFromAWS
{
  public static class Logger
    {
        private static string LogPath { get; set; }
        private static string LogFile { get; set; }
        private static string Smtp { get; set; }
        private static string From { get; set; }
        private static string To { get; set; }
        private static string Subject { get; set; }
        static Logger()
        {
            LogPath = ConfigurationManager.AppSettings["LogPath"].ToString();
            LogFile = ConfigurationManager.AppSettings["LogFile"].ToString();
            Smtp = ConfigurationManager.AppSettings["Smtp"].ToString();
            From = ConfigurationManager.AppSettings["From"].ToString();
            To = ConfigurationManager.AppSettings["To"].ToString();
            Subject = ConfigurationManager.AppSettings["Subject"].ToString();
        }
        public static void WriteLog(string log)
        {
            LogFile +=" - "+DateTime.Now.ToString("ddMMMyyyy") + ".txt";
            string FilePath = LogPath + "/" + LogFile;
            if (!File.Exists(FilePath))
            {
                File.Create(FilePath).Close();
            }
            
            File.AppendAllText(FilePath, WriteLogTemplate(log));

        }

        public static void SendLog(string Body)
        {
            try
            {
                //Log
                SmtpClient SmtpServer = new SmtpClient();
                MailMessage mail = new MailMessage();
                SmtpServer.Host = Smtp;

                mail = new MailMessage();
                mail.From = new MailAddress(From);

                string[] mailTo = To.Split(',');

                foreach (var s in mailTo)
                {
                    mail.To.Add(s);
                }


                mail.Subject = Subject;
                mail.Body = EmailLogTemplate(Body);

                SmtpServer.Send(mail);

            }
            catch (Exception d)
            {

                WriteLog(d.Message + Environment.NewLine + d.StackTrace);
            }
        }

        static string WriteLogTemplate(string log)
        {
            string template = "";

            template +="Log Start At : "+DateTime.Now+Environment.NewLine;
            template += log;
            template += Environment.NewLine + "Log End At : " + DateTime.Now + Environment.NewLine;

            return template;
        }

        static string EmailLogTemplate(string log)
        {
            string template = "";

            template += "Log Start At : " + DateTime.Now + Environment.NewLine;
            template += log;
            template += Environment.NewLine + "Log End At : " + DateTime.Now + Environment.NewLine;

            return template;
        }
    }
}
