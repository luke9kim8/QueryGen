select  
  cast(nullif(ref_0.C_PAYMENT_CNT,
    ref_0.C_ID) as INTEGER) as c0
from 
  main.CUSTOMER as ref_0
where ref_0.C_LAST is not NULL
limit 3;

