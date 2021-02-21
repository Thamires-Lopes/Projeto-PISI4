CREATE OR REPLACE STREAM "api_stream" (
    "DC_NOME" varchar(255), 
    "TEM_INS" DOUBLE,
    "UMD_INS" DOUBLE
);


CREATE OR REPLACE PUMP "api_pump" AS INSERT INTO "api_stream" SELECT STREAM 
    "DC_NOME", (("TEM_INS" * 1.8) + 32), ("UMD_INS" * 0.01)
    FROM "SOURCE_SQL_STREAM_001";


CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM" (
            "DC_NOME"  varchar(255),
            "HEAT_INDEX" DOUBLE
);

CREATE OR REPLACE PUMP "Output_Pump" AS INSERT INTO "DESTINATION_SQL_STREAM" SELECT STREAM "DC_NOME", 
        CASE
            WHEN (
                (1.1 * "TEM_INS")
                -10.3
                + (0.047 * "UMD_INS")
            ) < 80
            THEN (
                (1.1 * "TEM_INS")
                - 10.3
                + (0.047 * "UMD_INS")
            )
            WHEN (
                "TEM_INS" >= 80.0
                AND "TEM_INS" <= 112.0
                AND "UMD_INS" <= 0.13
            )
            THEN (
                (
                    - 42.379
                    + (2.04901523 * "TEM_INS")
                    + (10.14333127 * "UMD_INS")
                    - (0.22475541 * "TEM_INS" * "UMD_INS")
                    - (6.83783 * POWER(10, -3) * POWER("TEM_INS", 2))
                    - (5.481717 * POWER(10, -2) * POWER("UMD_INS", 2))
                    + (1.22874 * POWER(10, -3) * POWER("TEM_INS", 2) * "UMD_INS")
                    + (8.5282 * POWER(10, -4) * "TEM_INS" * POWER(10, 2))
                    - (1.99 * POWER(10, -6) * POWER("TEM_INS", 2) * POWER("UMD_INS", 2))
                )
                - (
                    (3.25 - (0.25 * "UMD_INS"))
                    * POWER((17 - ABS("TEM_INS" - 95)) / 17, 0.5)
                )
            )
            WHEN (
                "TEM_INS" >= 80.0
                AND "TEM_INS" <= 87.0
                AND "UMD_INS" > 0.85
            )
            THEN (
                (
                    - 42.379
                    + (2.04901523 * "TEM_INS")
                    + (10.14333127 * "UMD_INS")
                    - (0.22475541 * "TEM_INS" * "UMD_INS")
                    - (6.83783 * POWER(10, -3) * POWER("TEM_INS", 2))
                    - (5.481717 * POWER(10, -2) * POWER("UMD_INS", 2))
                    + (1.22874 * POWER(10, -3) * POWER("TEM_INS", 2) * "UMD_INS")
                    + (8.5282 * POWER(10, -4) * "TEM_INS" * POWER(10, 2))
                    - (1.99 * POWER(10, -6) * POWER("TEM_INS", 2) * POWER("UMD_INS", 2))
                )
                + (
                    0.02
                    * ("UMD_INS" - 85)
                    * (87 - "TEM_INS")
                )

            )
        ELSE (
                (
                    - 42.379
                    + (2.04901523 * "TEM_INS")
                    + (10.14333127 * "UMD_INS")
                    - (0.22475541 * "TEM_INS" * "UMD_INS")
                    - (6.83783 * POWER(10, -3) * POWER("TEM_INS", 2))
                    - (5.481717 * POWER(10, -2) * POWER("UMD_INS", 2))
                    + (1.22874 * POWER(10, -3) * POWER("TEM_INS", 2) * "UMD_INS")
                    + (8.5282 * POWER(10, -4) * "TEM_INS" * POWER(10, 2))
                    - (1.99 * POWER(10, -6) * POWER("TEM_INS", 2) * POWER("UMD_INS", 2))
                )
        )
        END
FROM "api_stream";
