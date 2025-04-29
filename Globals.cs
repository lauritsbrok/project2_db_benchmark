using DotNetEnv;

namespace project2_db_benchmark
{
    public static class Globals
    {
        public static string MANGO_DB_USERNAME { get; private set; }
        public static string MANGO_DB_PASSWORD { get; private set; }
        public static string MANGO_DB_NAME { get; private set; }
        public static string BUSINESS_JSON_FILE_NAME { get; private set; }
        public static string CHECKIN_JSON_FILE_NAME { get; private set; }
        public static string PHOTO_JSON_FILE_NAME { get; private set; }
        public static string REVIEW_JSON_FILE_NAME { get; private set; }
        public static string TIP_JSON_FILE_NAME { get; private set; }
        public static string USER_JSON_FILE_NAME { get; private set; }
        public static string POSTGRES_USER { get; private set; }
        public static string POSTGRES_PASSWORD { get; private set; }
        public static string POSTGRES_DB { get; private set; }

        public static void Init()
        {
            Env.Load();
            MANGO_DB_USERNAME = Environment.GetEnvironmentVariable("MONGO_INITDB_ROOT_USERNAME");
            MANGO_DB_PASSWORD = Environment.GetEnvironmentVariable("MONGO_INITDB_ROOT_PASSWORD");
            MANGO_DB_NAME = Environment.GetEnvironmentVariable("MONGO_DB_NAME");
            BUSINESS_JSON_FILE_NAME = Environment.GetEnvironmentVariable("BUSINESS_JSON_FILE_NAME");
            CHECKIN_JSON_FILE_NAME = Environment.GetEnvironmentVariable("CHECKIN_JSON_FILE_NAME");
            PHOTO_JSON_FILE_NAME = Environment.GetEnvironmentVariable("PHOTO_JSON_FILE_NAME");
            REVIEW_JSON_FILE_NAME = Environment.GetEnvironmentVariable("REVIEW_JSON_FILE_NAME");
            TIP_JSON_FILE_NAME = Environment.GetEnvironmentVariable("TIP_JSON_FILE_NAME");
            USER_JSON_FILE_NAME = Environment.GetEnvironmentVariable("USER_JSON_FILE_NAME");
            POSTGRES_USER = Environment.GetEnvironmentVariable("POSTGRES_USER");
            POSTGRES_PASSWORD = Environment.GetEnvironmentVariable("POSTGRES_PASSWORD");
            POSTGRES_DB = Environment.GetEnvironmentVariable("POSTGRES_DB");
        }
    }
}