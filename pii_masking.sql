
-- Masking policy for Address
CREATE OR REPLACE masking policy address_mask as (val string) returns string ->
    case
        when current_role() in ('SIG_PII') then val
        else '*********'
    end;

-- Masking policy for Phone Number
create or replace masking policy phone_mask as (val STRING) returns STRING ->
    case
        when current_role() in ('SIG_PII') then val
        else '*********'
    end;


-- Applying masking policy on Customer table in Raw Layer 
alter table if exists MOCK_PROJECT_DB.RAW.ORDER_SUMMARY 
modify column CUST_ADDRESS set masking policy address_mask;

alter table if exists MOCK_PROJECT_DB.RAW.ORDER_SUMMARY  
modify column PHONE set masking policy phone_mask;

-- Applying masking policy on Supplier table in Raw Layer 
alter table if exists MOCK_PROJECT_DB.RAW.PART_SUPPLY_SUPPLIER
modify column ADDRESS set masking policy address_mask;

alter table if exists MOCK_PROJECT_DB.RAW.PART_SUPPLY_SUPPLIER
modify column PHONE set masking policy phone_mask;
