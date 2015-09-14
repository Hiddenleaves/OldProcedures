CREATE OR REPLACE PROCEDURE SP_CAIWU_SAP_STOCK_DIANSHANG IS

  /*****************************
  给电商的条码汇总，读取SAP库存关联WMS条码
    *****************************/

  V_LM_QTY          NUMBER;
  V_COUNT_ITEM_CODE NUMBER;
  V_ADD_QTY         NUMBER;
  V_LM_CODE         VARCHAR2(60);
  V_LAST_DATE       DATE;
  V_MAX_BATCH       NUMBER;
  V_DATE            DATE;
  V_WEEK            VARCHAR2(60);
  V_DAY             VARCHAR2(60);
  V_COUNT_BATCH     NUMBER;

  --SAP3011的库存游标：
  CURSOR CUR_SAP_STOR_3011 IS
    select sap_company_code,
           sap_loc_code,
           item_code,
           sum(sap_stor_qty) as sap_stor_qty
      from (SELECT T.WERKS AS sap_company_code,
                   T.LGORT AS sap_loc_code,
                   T.MATNR AS item_code,
                   T.LABST + T.speme AS sap_stor_qty
              FROM mard@opple_sap T
             WHERE T.LGORT = '3011'
               AND T.WERKS = '6000'
               AND T.LABST + T.speme <> '0')
     group by sap_company_code, item_code, sap_loc_code;

  --SAP8018的库存游标：
  CURSOR CUR_SAP_STOR_8018 IS
    select sap_company_code,
           sap_loc_code,
           item_code,
           sum(sap_stor_qty) as sap_stor_qty
      from (SELECT T.WERKS AS sap_company_code,
                   T.LGORT AS sap_loc_code,
                   T.MATNR AS item_code,
                   T.LABST + T.speme AS sap_stor_qty
              FROM mard@opple_sap T
             WHERE T.LGORT = '8018'
               AND T.WERKS = '6000'
               AND T.LABST + T.speme <> '0')
     group by sap_company_code, item_code, sap_loc_code;

  V_SAP_COMPANY_CODE INV_ITEM.SAP_COMPANY_CODE%TYPE;
  V_SAP_LOC_CODE     INV_ITEM.SAP_LOC_CODE%TYPE;
  V_SAP_ITEM_CODE    INV_ITEM.ITEM_CODE%TYPE;
  V_SAP_STOR_QTY     INV_ITEM.STOR_QTY%TYPE;

BEGIN

  SELECT SYSDATE INTO V_DATE FROM DUAL;
  SELECT TO_CHAR(SYSDATE, 'ww') INTO V_WEEK FROM DUAL;
  V_DAY := '061010';

  --查出上次插入电商数据表的最后时间：
  SELECT MAX(CREATED_DTM_LOC) INTO V_LAST_DATE FROM TEMP_OUTBOUND_LIST_DS;

  --把大于这个时间的出库条码数据同步过来：
  INSERT INTO TEMP_OUTBOUND_LIST_DS
    (ITEM_CODE,
     LM_CODE,
     BPP_CODE,
     BATCH,
     OUTBOUND_QTY,
     CREATED_DTM_LOC,
     LIST_ID)
    SELECT ITEM_CODE,
           LM_CODE,
           BPP_CODE,
           BATCH,
           OUTBOUND_QTY,
           CREATED_DTM_LOC,
           OUTBOUND_ORDER_LIST_ID
      FROM OUTBOUND_ORDER_LIST
     WHERE BPP_CODE IN ('60003011', '60008018')
       AND CREATED_DTM_LOC > V_LAST_DATE;

  --修改批次为空的条码的批次：
  update TEMP_OUTBOUND_LIST_DS a
     set a.batch = (select b.batch
                      from (select lm_code, batch
                              from barcode_list
                             where lm_code in
                                   (select lm_code
                                      from TEMP_OUTBOUND_LIST_DS
                                     where batch is null
                                       and CREATED_DTM_LOC > V_LAST_DATE)
                               and batch is not null) b
                     where a.lm_code = b.lm_code
                       and rownum = 1)
   where batch is null
     and CREATED_DTM_LOC > V_LAST_DATE;

  --先清空条码临时表：
  DELETE FROM TEMP_OUTBOUND_ORDER_BATCH;

  --把本次要跑的款料条码数据汇总插入临时表：
  INSERT INTO TEMP_OUTBOUND_ORDER_BATCH
    SELECT ITEM_CODE,
           max(lm_code),
           BPP_CODE,
           BATCH,
           sum(outbound_qty),
           sysdate,
           sys_guid()
      FROM TEMP_OUTBOUND_LIST_DS
     WHERE ITEM_CODE in (SELECT distinct T.MATNR AS item_code
                           FROM mard@opple_sap T
                          WHERE T.LGORT in ('3011', '8018')
                            AND T.WERKS = '6000'
                            AND T.LABST + T.speme <> '0')
       AND BATCH IS NOT NULL
     group by item_code, bpp_code, batch;

  --打开SAP库存的游标遍历：
  OPEN CUR_SAP_STOR_3011;

  LOOP
    FETCH CUR_SAP_STOR_3011
      INTO V_SAP_COMPANY_CODE, V_SAP_LOC_CODE, V_SAP_ITEM_CODE, V_SAP_STOR_QTY;
    EXIT WHEN CUR_SAP_STOR_3011%NOTFOUND;
  
    --查询行数：
    SELECT COUNT(*)
      INTO V_COUNT_ITEM_CODE
      FROM TEMP_OUTBOUND_ORDER_BATCH
     where item_code = V_SAP_ITEM_CODE;
  
    --初始化几个库存值：
    V_ADD_QTY := 0;
    V_LM_QTY  := 0;
  
    WHILE V_ADD_QTY < V_SAP_STOR_QTY AND V_COUNT_ITEM_CODE > 0 LOOP
    
      --查询出最大的批次是多少：
      SELECT MAX(batch)
        INTO V_MAX_BATCH
        FROM TEMP_OUTBOUND_ORDER_BATCH
       WHERE item_code = V_SAP_ITEM_CODE;
    
      --查询这个批次的第一条数据的数量是多少：
      SELECT SUM(OUTBOUND_QTY), ITEM_CODE, COUNT(1)
        INTO V_LM_QTY, V_LM_CODE, V_COUNT_BATCH
        FROM TEMP_OUTBOUND_ORDER_BATCH
       WHERE BATCH = V_MAX_BATCH
         and item_code = V_SAP_ITEM_CODE
       GROUP BY ITEM_CODE;
    
      --如果总数量超过了SAP库存数，则把此条码中物料的数量算成剩下的数量
      IF V_SAP_STOR_QTY < V_LM_QTY + V_ADD_QTY THEN
        V_LM_QTY := V_SAP_STOR_QTY - V_ADD_QTY;
      END IF;
    
      --把这个批次第一条数据插入SAP库存的条码表：
      INSERT INTO TEMP_SAP_STOR_AGE
        (ITEM_CODE,
         LM_CODE,
         BPP_CODE,
         BATCH,
         OUTBOUND_QTY,
         CREATED_DTM_LOC)
      VALUES
        (V_SAP_ITEM_CODE,
         'MANY_LM_CODE',
         V_SAP_LOC_CODE,
         V_MAX_BATCH,
         V_LM_QTY,
         V_DATE);
    
      --如果总数量超过了SAP库存数，则把此条码中物料的数量算成剩下的数量
      IF V_SAP_STOR_QTY < V_LM_QTY + V_ADD_QTY THEN
      
        --把这个批次的数量减掉：
        update TEMP_OUTBOUND_ORDER_BATCH
           SET outbound_QTY = outbound_QTY - V_LM_QTY
         WHERE BATCH = V_MAX_BATCH
           and item_code = V_SAP_ITEM_CODE;
      
      else
      
        --把这个批次第一条数据删掉，并总数减1：
        delete from TEMP_OUTBOUND_ORDER_BATCH
         WHERE BATCH = V_MAX_BATCH
           and item_code = V_SAP_ITEM_CODE;
        V_COUNT_ITEM_CODE := V_COUNT_ITEM_CODE - 1;
      
      end if;
    
      --总数累加：
      V_ADD_QTY := V_LM_QTY + V_ADD_QTY;
    
    END LOOP;
  
    --如果SAP库存有，WMS出库条码不够，直接插入一条数据
    IF V_ADD_QTY < V_SAP_STOR_QTY AND V_COUNT_ITEM_CODE = 0 THEN
    
      V_LM_QTY := V_SAP_STOR_QTY - V_ADD_QTY;
    
      --把这个批次第一条数据插入SAP库存的条码表：
      INSERT INTO TEMP_SAP_STOR_AGE
        (ITEM_CODE,
         LM_CODE,
         BPP_CODE,
         BATCH,
         OUTBOUND_QTY,
         CREATED_DTM_LOC)
      VALUES
        (V_SAP_ITEM_CODE,
         'NO_LM_CODE',
         V_SAP_LOC_CODE,
         V_DAY,
         V_LM_QTY,
         V_DATE);
    
    END IF;
  
  END LOOP;
  CLOSE CUR_SAP_STOR_3011;

  --打开SAP库存的游标遍历：
  OPEN CUR_SAP_STOR_8018;

  LOOP
    FETCH CUR_SAP_STOR_8018
      INTO V_SAP_COMPANY_CODE, V_SAP_LOC_CODE, V_SAP_ITEM_CODE, V_SAP_STOR_QTY;
    EXIT WHEN CUR_SAP_STOR_8018%NOTFOUND;
  
    --查询行数：
    SELECT COUNT(*)
      INTO V_COUNT_ITEM_CODE
      FROM TEMP_OUTBOUND_ORDER_BATCH
     where item_code = V_SAP_ITEM_CODE;
  
    --初始化几个库存值：
    V_ADD_QTY := 0;
    V_LM_QTY  := 0;
  
    WHILE V_ADD_QTY < V_SAP_STOR_QTY AND V_COUNT_ITEM_CODE > 0 LOOP
    
      --查询出最大的批次是多少：
      SELECT MAX(batch)
        INTO V_MAX_BATCH
        FROM TEMP_OUTBOUND_ORDER_BATCH
       WHERE item_code = V_SAP_ITEM_CODE;
    
      --查询这个批次的第一条数据的数量是多少：
      SELECT SUM(OUTBOUND_QTY), ITEM_CODE, COUNT(1)
        INTO V_LM_QTY, V_LM_CODE, V_COUNT_BATCH
        FROM TEMP_OUTBOUND_ORDER_BATCH
       WHERE BATCH = V_MAX_BATCH
         and item_code = V_SAP_ITEM_CODE
       GROUP BY ITEM_CODE;
    
      --如果总数量超过了SAP库存数，则把此条码中物料的数量算成剩下的数量
      IF V_SAP_STOR_QTY < V_LM_QTY + V_ADD_QTY THEN
        V_LM_QTY := V_SAP_STOR_QTY - V_ADD_QTY;
      END IF;
    
      --把这个批次第一条数据插入SAP库存的条码表：
      INSERT INTO TEMP_SAP_STOR_AGE
        (ITEM_CODE,
         LM_CODE,
         BPP_CODE,
         BATCH,
         OUTBOUND_QTY,
         CREATED_DTM_LOC)
      VALUES
        (V_SAP_ITEM_CODE,
         'MANY_LM_CODE',
         V_SAP_LOC_CODE,
         V_MAX_BATCH,
         V_LM_QTY,
         V_DATE);
    
      --如果总数量超过了SAP库存数，则把此条码中物料的数量算成剩下的数量
      IF V_SAP_STOR_QTY < V_LM_QTY + V_ADD_QTY THEN
      
        --把这个批次的数量减掉：
        update TEMP_OUTBOUND_ORDER_BATCH
           SET outbound_QTY = outbound_QTY - V_LM_QTY
         WHERE BATCH = V_MAX_BATCH
           and item_code = V_SAP_ITEM_CODE;
      
      else
      
        --把这个批次第一条数据删掉，并总数减1：
        delete from TEMP_OUTBOUND_ORDER_BATCH
         WHERE BATCH = V_MAX_BATCH
           and item_code = V_SAP_ITEM_CODE;
        V_COUNT_ITEM_CODE := V_COUNT_ITEM_CODE - 1;
      
      end if;
    
      --总数累加：
      V_ADD_QTY := V_LM_QTY + V_ADD_QTY;
    
    END LOOP;
  
    --如果SAP库存有，WMS出库条码不够，直接插入一条数据
    IF V_ADD_QTY < V_SAP_STOR_QTY AND V_COUNT_ITEM_CODE = 0 THEN
    
      V_LM_QTY := V_SAP_STOR_QTY - V_ADD_QTY;
    
      --把这个批次第一条数据插入SAP库存的条码表：
      INSERT INTO TEMP_SAP_STOR_AGE
        (ITEM_CODE,
         LM_CODE,
         BPP_CODE,
         BATCH,
         OUTBOUND_QTY,
         CREATED_DTM_LOC)
      VALUES
        (V_SAP_ITEM_CODE,
         'NO_LM_CODE',
         V_SAP_LOC_CODE,
         V_DAY,
         V_LM_QTY,
         V_DATE);
    
    END IF;
  
  END LOOP;
  CLOSE CUR_SAP_STOR_8018;

  commit;

END SP_CAIWU_SAP_STOCK_DIANSHANG;
