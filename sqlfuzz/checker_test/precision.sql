--14779

select  
  ref_0.W_STREET_1 as c0, 
  ref_0.W_YTD as c1, 
  ref_0.W_NAME as c2
from 
  main.WAREHOUSE as ref_0
where (25 is not NULL) 
  and ((ref_0.W_STATE is not NULL) 
    or (ref_0.W_CITY is not NULL));

