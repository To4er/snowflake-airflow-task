LOAD_BULK="""
            COPY INTO AIRLINE_DB.RAW.PASSENGERS_RAW (
                PASSENGER_ID, FIRST_NAME, LAST_NAME, GENDER, AGE, 
                NATIONALITY, AIRPORT_NAME, AIRPORT_COUNTRY_CODE, COUNTRY_NAME, 
                AIRPORT_CONTINENT, CONTINENTS, DEPARTURE_DATE, ARRIVAL_AIRPORT, 
                PILOT_NAME, FLIGHT_STATUS, TICKET_TYPE, PASSENGER_STATUS
            )
            FROM (
                SELECT 
                    t.$2  AS PASSENGER_ID,
                    t.$3  AS FIRST_NAME,
                    t.$4  AS LAST_NAME,
                    t.$5  AS GENDER,
                    t.$6  AS AGE,
                    t.$7  AS NATIONALITY,
                    t.$8  AS AIRPORT_NAME,
                    t.$9  AS AIRPORT_COUNTRY_CODE,
                    t.$10 AS COUNTRY_NAME,
                    t.$11 AS AIRPORT_CONTINENT,
                    t.$12 AS CONTINENTS,
                    t.$13 AS DEPARTURE_DATE,
                    t.$14 AS ARRIVAL_AIRPORT,
                    t.$15 AS PILOT_NAME,
                    t.$16 AS FLIGHT_STATUS,
                    t.$17 AS TICKET_TYPE,
                    t.$18 AS PASSENGER_STATUS
                FROM @AIRLINE_DB.RAW.MY_INT_STAGE t
            )
            FILE_FORMAT = (
                TYPE = 'CSV' 
                SKIP_HEADER = 1 
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            )
            ON_ERROR = 'CONTINUE'
            FORCE = TRUE;
        """

PROC_RAW_TO_DWH = "CALL PROC_RAW_TO_DWH();"
PROC_DWH_TO_DM = "CALL PROC_DWH_TO_DM();"

def upload_files(file_path: str):
    return f"PUT file://{file_path} @AIRLINE_DB.RAW.MY_INT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"