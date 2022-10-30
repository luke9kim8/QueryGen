--18711

select  
  ref_0.C_PAYMENT_CNT as c0, 
  ref_0.C_DELIVERY_CNT as c1, 
  ref_0.C_BALANCE as c2
from 
  main.CUSTOMER as ref_0
where ref_0.C_ID is not NULL;

