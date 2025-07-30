package job

/*
create a job that rusn every 5 seconds to check if the GET os.Getenv("PROCESSOR_DEFAULT_URL")/service-health and os.Getenv("PROCESSOR_FALLBACK_URL")/service-health
these endpoints will return 200 and a json with the struct:
HTTP 200 - Ok
{
    "failing": false,
    "minResponseTime": 100
}
where failing represents if the service is up or down and minResponseTime is the minimum response time in milliseconds

based on the responses I want to update the variables primaryProcessor and fallbackProcessor with the respective URLs so then the worker can use the correct processor
*/
