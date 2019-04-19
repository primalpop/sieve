-- Conditionally drop the function when it exists.
DROP FUNCTION IF EXISTS pcheck;

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
        DECLARE satisfied INT;
        DECLARE uSat INT;
        DECLARE lSat INT;
        DECLARE tsSat INT;
        DECLARE tempSat INT;
        DECLARE enSat INT;
        DECLARE aSat INT;
        DECLARE cursor1 CURSOR
        FOR SELECT uPol, lPol, templPol, tempgPol, elPol, egPol, aPol, tslPol, tsgPol
        FROM SIMPLE_POLICY
        where querier = que AND owner = user_id AND purpose = pur;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
        SET satisfied = -1;
        SET uSat = -1;
        SET lSat = -1;
        SET tsSat = -1;
        SET tempSat = -1;
        SET enSat = -1;
        SET aSat = -1;
        OPEN cursor1;
        read_loop: loop
            fetch cursor1 into uVal, lVal, templVal, tempgVal, elVal, egVal, aVal, tslVal, tsgVal;
            IF uVal is NOT NULL AND uVal = user_id THEN 
                SET uSat = 1;
            ELSE 
                IF uVal is NULL THEN set uSat = 1;
                ELSE SET uSat = -1;
                END IF;
            END IF;
            IF lVal is NOT NULL AND lVal = location THEN 
                SET lSat = 1;
            ELSE 
                IF lVal is NULL THEN set lSat = 1;
                ELSE SET lSat = -1;
                END IF;
            END IF;
            IF templVal is NOT NULL AND tempgVal is NOT NULL AND templVal <= temperature AND
                    tempgVal >= temperature THEN 
                SET tempSat = 1;
            ELSE 
                IF templVal is NULL OR tempgVal is NULL THEN set tempSat = 1;
                ELSE SET tempSat = -1;
                END IF;
            END IF;
            IF elVal is NOT NULL AND egVal is NOT NULL AND elVal <= energy AND
                    egVal >= energy THEN 
                SET enSat = 1;
            ELSE 
                IF elVal is NULL OR egVal is NULL THEN set enSat = 1;
                ELSE SET enSat = -1;
                END IF;
            END IF;   
            IF tslVal is NOT NULL AND tsgVal is NOT NULL AND tslVal <= ts AND
                    tsgVal >= ts THEN 
                SET tsSat = 1;
            ELSE 
                IF tslVal is NULL OR tsgVal is NULL THEN set tsSat = 1;
                ELSE SET tsSat = -1;
                END IF;
            END IF;
            IF aVal is NOT NULL AND aVal = activity THEN 
                SET aSat = 1;
            ELSE 
                IF aVal is NULL THEN set aSat = 1;
                ELSE SET aSat = -1;
                END IF;
            END IF;
            IF done THEN
                LEAVE read_loop;
            END IF;
            IF(uSat = 1 AND lSat = 1 AND tempSat = 1 AND enSat = 1 AND tsSat = 1 AND aSat = 1) THEN
                SET satisfied = 1;
                RETURN satisfied;
            END IF;   
        END loop;
        close cursor1;
        RETURN satisfied;
    END//

DELIMITER ;