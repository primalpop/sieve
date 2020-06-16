use realtest;
-- Conditionally drop the function when it exists.
DROP FUNCTION IF EXISTS hybcheck;

DELIMITER //

-- https://stackoverflow.com/questions/24435138/returning-a-value-from-a-mysql-function-after-iterating-cursor
-- https://dev.mysql.com/doc/refman/8.0/en/cursors.html
-- https://dev.mysql.com/doc/refman/8.0/en/flow-control-statements.html

CREATE FUNCTION hybcheck(querier INT, guard_id varchar(255), user_id INT, location_id VARCHAR(255),
	start_date date, start_time time, user_profile VARCHAR(16), user_group VARCHAR(16)) RETURNS INT
    DETERMINISTIC
    BEGIN
        DECLARE done INT DEFAULT FALSE;
        DECLARE uVal INT;
        DECLARE profVal VARCHAR(255);
        DECLARE groupVal VARCHAR(255);
        DECLARE lVal VARCHAR(255);
        DECLARE sdlVal DATE;
        DECLARE sdgVal DATE;
        DECLARE stlVal TIME;
        DECLARE stgVal TIME;
        DECLARE enf VARCHAR(255);
        DECLARE satisfied INT DEFAULT -1;
        DECLARE uSat INT DEFAULT -1;
        DECLARE profSat INT DEFAULT -1;
        DECLARE groupSAT INT DEFAULT -1;
        DECLARE lSat INT DEFAULT -1;
        DECLARE dateSat INT DEFAULT -1;
        DECLARE timeSat INT DEFAULT -1;
        DECLARE policyCount BIGINT DEFAULT 0;
        DECLARE res_tuple VARCHAR(255);
        DECLARE cursor1 CURSOR
        FOR SELECT ownerEq, profEq, groupEq, locEq, dateGe, dateLe, timeGe, timeLe
        FROM realtest.FLAT_POLICY as fp, USER_GUARD_TO_POLICY as ugp
        WHERE fp.querier = querier
        and ugp.policy_id = guard_id
        and fp.profEq = user_profile
        and fp.groupEq = user_group
        and fp.locEq = location_id
        and fp.dateGe <= start_date
        and fp.dateLe >= start_date
        and fp.timeGe <= start_time
        and fp.timeLe >= start_time;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
        OPEN cursor1;
        read_loop: loop
            fetch cursor1 into uVal, profVal, groupVal, lVal, sdlVal, sdgVal, stlVal, stgVal;
            IF done THEN
                LEAVE read_loop;
            END IF;
            SET policyCount = policyCount + 1;
            IF uVal is NOT NULL AND uVal = user_id THEN
                SET uSat = 1;
            ELSE
                IF uVal is NULL THEN set uSat = 1;
                ELSE SET uSat = -1;
                END IF;
            END IF;
            IF lVal is NOT NULL AND lVal = location_id THEN
                SET lSat = 1;
            ELSE
                IF lVal is NULL THEN set lSat = 1;
                ELSE SET lSat = -1;
                END IF;
            END IF;
            IF profVal is NOT NULL AND profVal = user_profile THEN
                SET profSat = 1;
            ELSE
                IF profSat is NULL THEN set profSat = 1;
                ELSE SET profSat = -1;
                END IF;
            END IF;
            IF groupVal is NOT NULL AND groupVal = user_group THEN
                SET groupSAT = 1;
            ELSE
                IF groupVal is NULL THEN set groupSAT = 1;
                ELSE SET groupSAT = -1;
                END IF;
            END IF;
            IF sdlVal is NOT NULL AND sdgVal is NOT NULL AND sdlVal <= start_date AND
                        sdgVal >= start_date THEN
                SET dateSat = 1;
            ELSE
                IF sdlVal is NULL OR sdgVal is NULL THEN set dateSat = 1;
                ELSE SET dateSat = -1;
                END IF;
            END IF;
            IF stlVal is NOT NULL AND stgVal is NOT NULL AND stlVal <= start_time AND
                        stgVal >= start_time THEN
                    SET timeSat = 1;
                ELSE
                    IF stlVal is NULL OR stgVal is NULL THEN set timeSat = 1;
                    ELSE SET timeSat = -1;
                    END IF;
                END IF;
            IF(uSat = 1 AND lSat = 1 AND dateSat = 1 AND timeSat = 1 AND profSat = 1 AND groupSAT = 1) THEN
                SET satisfied = 1;
                LEAVE read_loop;
            END IF;
        END loop;
        close cursor1;
        RETURN satisfied;
    END//
DELIMITER ;
