DELIMITER //

-- https://stackoverflow.com/questions/24435138/returning-a-value-from-a-mysql-function-after-iterating-cursor
-- https://dev.mysql.com/doc/refman/8.0/en/cursors.html
-- https://dev.mysql.com/doc/refman/8.0/en/flow-control-statements.html

CREATE FUNCTION pcheck(que VARCHAR(255), pur VARCHAR(255), user_id VARCHAR(255), 
    location VARCHAR(255), ts timestamp, temperature VARCHAR(255), energy VARCHAR(255), 
    activity VARCHAR(255)) RETURNS INT
    DETERMINISTIC
    BEGIN
        DECLARE done INT DEFAULT FALSE;
        DECLARE uVal VARCHAR(255);
        DECLARE lVal VARCHAR(255);
        DECLARE templVal VARCHAR(255);
        DECLARE tempgVal VARCHAR(255);
        DECLARE elVal VARCHAR(255);
        DECLARE egVal VARCHAR(255);
        DECLARE aVal VARCHAR(255);
        DECLARE tslVal TIMESTAMP;
        DECLARE tsgVal TIMESTAMP;
        DECLARE policyCount INT;
        DECLARE satisfied INT;
        DECLARE cursor1 CURSOR
        FOR SELECT uPol, lPol, templPol, tempgPol, elPol, egPol, aPol, tslPol, tsgPol
        FROM SIMPLE_POLICY
        where querier = que AND owner = user_id AND purpose = pur;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
        SET policyCount = 0;
        SET satisfied = 1;
        OPEN cursor1;
        read_loop: loop
            fetch cursor1 into uVal, lVal, templVal, tempgVal, elVal, egVal, aVal, tslVal, tsgVal;
            IF uVal is NOT NULL AND uVal != user_id THEN 
                SET satisfied = 0;
                ITERATE read_loop;
            END IF;
            IF lVal is NOT NULL AND lVal != location THEN 
                SET satisfied = 0;
                ITERATE read_loop;
            END IF;
            IF templVal is NOT NULL AND tempgVal is NOT NULL AND NOT (templVal <= temperature AND
                    tempgVal >= temperature) THEN 
                SET satisfied = 0;
                ITERATE read_loop;
            END IF;
            IF elVal is NOT NULL AND egVal is NOT NULL AND NOT (elVal <= energy AND
                    egVal >= energy) THEN 
                SET satisfied = 0;
                ITERATE read_loop;
            END IF;
            IF tslVal is NOT NULL AND tsgVal is NOT NULL AND NOT (tslVal <= ts AND
                    tsgVal >= ts) THEN 
                SET satisfied = 0;
                ITERATE read_loop;
            END IF;
            IF aVal is NOT NULL AND aVal != activity THEN 
                SET satisfied = 0;
                ITERATE read_loop;
            END IF;
            IF done THEN
                LEAVE read_loop;
            END IF;
            IF(satisfied = 1) THEN
                RETURN satisfied;
            ELSE
                SET satisfied = 1;
                ITERATE read_loop; 
            END IF;   
            SET policyCount = policyCount + 1;
        END loop;
        close cursor1;
        RETURN satisfied;
    END//

DELIMITER ;