#region Library
using System;
using System.Collections.Generic;
using System.Text;
using System.Configuration;
using Amazon.S3;
using Amazon;
using Amazon.S3.Model;
using System.IO;
using System.Threading.Tasks;
#endregion
namespace FetchDataFromAWS
{
    public class Fetcher
    {
        #region Global Declaration

        string accessKey { get; set; }
         string secretKey { get; set; }
         string bucketName { get; set; }
         string regionName { get; set; }
         string storingDirectory { get; set; }
         string dataDirectory { get; set; }
         string parsedDirectory { get; set; }
        StringBuilder Logstring = null;
        AmazonS3Config config = null;
        AmazonS3Client s3Client = null;

        #endregion

        #region Constructor
        public Fetcher()
        {
            accessKey = ConfigurationManager.AppSettings["accessKey"].ToString();
            secretKey = ConfigurationManager.AppSettings["secretKey"].ToString();
            bucketName = ConfigurationManager.AppSettings["bucketName"].ToString();
            regionName = ConfigurationManager.AppSettings["regionName"].ToString();
            storingDirectory = ConfigurationManager.AppSettings["storingDirectory"].ToString();
            dataDirectory = ConfigurationManager.AppSettings["dataDirectory"].ToString();
            parsedDirectory = ConfigurationManager.AppSettings["parsedDirectory"].ToString();
            Logstring = new StringBuilder();
            config = new AmazonS3Config();
            
        }

        #endregion

        #region Fetching Data
        public string FetchFolders()
        {
            string returnstring = "Success";
            try
            {
                #region Creating AmazonS3Client Instance

                config.RegionEndpoint = RegionEndpoint.GetBySystemName(regionName);
                s3Client = new AmazonS3Client(accessKey, secretKey, config);

                #endregion

                #region Get All S3 Objects From dataDirectory

                ListObjectsRequest request = new ListObjectsRequest();
                request.BucketName = bucketName;
                request.Prefix = dataDirectory; 
                Task<ListObjectsResponse> response = s3Client.ListObjectsAsync(request);
                response.Wait();
                ListObjectsResponse responsez = new ListObjectsResponse();
                responsez = response.Result;

                #endregion

                #region Enumeration S3 Objects


                foreach (var objt in responsez.S3Objects)
                {
                    string key = "";
                    try
                    {
                        Console.WriteLine("{0}\t{1}", objt.Key, objt.LastModified);
                        key = objt.Key;
                        if (objt.Key.Contains("csv"))
                        {
                            #region Get S3 Object Data

                            
                            GetObjectRequest request1 = new GetObjectRequest();
                            request1.BucketName = bucketName + "/" + dataDirectory;
                            request1.Key = objt.Key.Replace(dataDirectory+"/","");
                            Task<GetObjectResponse> response1 = s3Client.GetObjectAsync(request1);
                            response1.Wait();
                            GetObjectResponse getObjectResponse = response1.Result;

                            #endregion

                            #region Storing and Moving S3 Object Data 

                            using (Stream responseStream = getObjectResponse.ResponseStream)

                            using (StreamReader reader = new StreamReader(responseStream))
                            {
                                string contentType = getObjectResponse.Headers["Content-Type"];
                                string responseBody = reader.ReadToEnd();
                                string FileCategory = request1.Key.Split(new string[] { "-Delta" }, StringSplitOptions.None)[0];
                                if (!Directory.Exists(storingDirectory + "/" + FileCategory))
                                        Directory.CreateDirectory(storingDirectory + "/" + FileCategory);
                                File.WriteAllText(storingDirectory + "/"+ FileCategory + "/" + request1.Key, responseBody);


                                #region Calling Function For Moving S3 Object From dataDirectory To parsedDirectory

                                Task MoveFileTask =MoveObjectAsync(request1.BucketName, request1.Key, bucketName+"/"+parsedDirectory, request1.Key);
                                MoveFileTask.Wait();

                                #endregion

                            }

                            #endregion
                        }
                    }
                    catch (Exception ex)
                    {
                        appendlog("Inner Loop Break For Key : "+ key+Environment.NewLine+ ex.Message + Environment.StackTrace + ex.StackTrace);
                    }
                }

                #endregion

            }
            catch (Exception ex)
            {
                returnstring = ex.Message + Environment.NewLine + ex.StackTrace;
                appendlog("Main Try Break : "+Environment.NewLine+ex.Message + Environment.StackTrace + ex.StackTrace);
            }
            finally
            {
                if(Logstring.Length>0)
                {
                    Logger.WriteLog(Logstring.ToString());
                    Logger.SendLog(Logstring.ToString());
                    
                }
            }
            return returnstring;
        }

        #endregion


        #region Private Functionalities

        private void appendlog(string log)
        {
            Logstring.Append(log);
        }

        private async Task MoveObjectAsync(string SourceBucket, string SourceKey, string DestinationBucket, string DestinationKey)
        {
            bool IsCopied = false;
            try
            {
                #region Copy Data From Source To Destination

                
                CopyObjectRequest Crequest = new CopyObjectRequest
                {
                    SourceBucket = SourceBucket,
                    SourceKey = SourceKey,
                    DestinationBucket = DestinationBucket,
                    DestinationKey = DestinationKey
                };
                CopyObjectResponse Cresponse = await s3Client.CopyObjectAsync(Crequest);
                IsCopied = true;

                #endregion

                #region Deleting Data From Source

              
                var CNrequest = new DeleteObjectRequest
                {
                    BucketName = SourceBucket,
                    Key = SourceKey
                };
                var CNresponse = await s3Client.DeleteObjectAsync(CNrequest);

                #endregion
            }
            catch (Exception ex)
            {
                appendlog(Environment.NewLine +"Moving Object Break " +Environment.NewLine+ "IsCopied : "+ IsCopied );
                appendlog(Environment.NewLine+ "SourceBucket : "+ SourceBucket + "  SourceKey : "+ SourceKey+ "  DestinationBucket : "+ DestinationBucket+ "  DestinationKey : "+ DestinationKey);
                appendlog(Environment.NewLine+ex.Message + Environment.StackTrace + ex.StackTrace);

            }
        }

        #endregion
    }
}
