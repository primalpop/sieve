

CREATE FUNCTION pcheck(querier int, user_id int, location_id varchar(255), start_date date, start_time time, user_profile varchar(16), user_group varchar(16)) returns int deterministic
BEGIN
   DECLARE done int DEFAULT false;
declare uval int;
declare profval varchar(255);
declare groupval varchar(255);
declare lval varchar(255);
declare sdlval date;
declare sdgval date;
declare stlval time;
declare stgval time;
declare enf varchar(255);
declare satisfied int DEFAULT - 1;
declare usat int DEFAULT - 1;
declare profsat int DEFAULT - 1;
declare groupsat int DEFAULT - 1;
declare lsat int DEFAULT - 1;
declare datesat int DEFAULT - 1;
declare timesat int DEFAULT - 1;
declare policycount bigint DEFAULT 0;
declare res_tuple varchar(255);
declare cursor1 CURSOR FOR
SELECT
   ownereq,
   profeq,
   groupeq,
   loceq,
   datege,
   datele,
   timege,
   timele
FROM
   realtest.flat_policy AS fp
WHERE
   fp.querier = querier
   AND fp.ownereq = user_id
   AND fp.profeq = user_profile
   AND fp.groupeq = user_group
   AND fp.loceq = location_id
   AND fp.datege <= start_date
   AND fp.datele >= start_date
   AND fp.timege <= start_time
   AND fp.timele >= start_time;
declare CONTINUE handler FOR NOT found
SET
   done = true;
open cursor1;
READ_LOOP: loop FETCH cursor1 INTO uval,
profval,
groupval,
lval,
sdlval,
sdgval,
stlval,
stgval;
if done
THEN
   leave read_loop;
endIF;
SET
   policycount = policycount + 1;
IF uval IS NOT NULL
AND uval = user_id
then
SET
   usat = 1;
else
   IF uval IS NULL
THEN
SET
   usat = 1;
else
SET
   usat = - 1;
ENDIF;
ENDIF;
IF lval IS NOT NULL
AND lval = location_id
then
SET
   lsat = 1;
else
   IF lval IS NULL
THEN
SET
   lsat = 1;
else
SET
   lsat = - 1;
ENDIF;
ENDIF;
IF profval IS NOT NULL
AND profval = user_profile
then
SET
   profsat = 1;
else
   IF profsat IS NULL
THEN
SET
   profsat = 1;
else
SET
   profsat = - 1;
ENDIF;
ENDIF;
IF groupval IS NOT NULL
AND groupval = user_group
then
SET
   groupsat = 1;
else
   IF groupval IS NULL
THEN
SET
   groupsat = 1;
else
SET
   groupsat = - 1;
ENDIF;
ENDIF;
IF sdlval IS NOT NULL
AND sdgval IS NOT NULL
AND sdlval <= start_date
AND sdgval >= start_date
then
SET
   datesat = 1;
else
   IF sdlval IS NULL
   OR sdgval IS NULL
THEN
SET
   datesat = 1;
else
SET
   datesat = - 1;
ENDIF;
ENDIF;
IF stlval IS NOT NULL
AND stgval IS NOT NULL
AND stlval <= start_time
AND stgval >= start_time
then
SET
   timesat = 1;
else
   IF stlval IS NULL
   OR stgval IS NULL
THEN
SET
   timesat = 1;
else
SET
   timesat = - 1;
ENDIF;
ENDIF;
IF(usat = 1
AND lsat = 1
AND datesat = 1
AND timesat = 1
AND profsat = 1
AND groupsat = 1)
then
SET
   satisfied = 1;
LEAVE read_loop;
ENDIF;
END
loop;
CLOSE cursor1;
RETURN satisfied;
END
 //

