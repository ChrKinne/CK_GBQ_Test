CREATE OR REPLACE TABLE `{{ params.project }}.{{ params.environment }}_DWH_GBQ_Test.CK_GBQ_Test`
AS
SELECT * FROM `{{ params.project }}.{{ params.environment }}_LDZ_GBQ_Test.CK_GBQ_Test`
