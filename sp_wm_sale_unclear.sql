create or replace procedure sp_wm_sale_unclear is

  --建立计算SAP未清订单数量的游标：
  cursor cur_wm_unclear is
    select * from inv_wm_unclear_temp where get_inv_time >= trunc(sysdate);

  type rec_wm_unclear is table of cur_wm_unclear%rowtype;
  r_wm_unclear rec_wm_unclear;

  --建立分摊WMS库龄库存的订单游标：
  cursor cur_wm_kucun is
    select *
      from inv_wm_unclear_temp
     where get_inv_time >= trunc(sysdate)
     order by oem_date;

  type rec_wm_kucun is table of cur_wm_kucun%rowtype;
  r_wm_kucun rec_wm_kucun;

  v_ext_qty    number;
  v_date       date;
  v_bpp_code   varchar2(300);
  v_item_code  varchar2(100);
  v_zone_code  varchar2(100);
  v_item_count number;

  v_add_qty    number;
  v_stor_qty   number;
  v_paixu_max  number;
  v_stor_bpp   varchar2(300);
  v_wms_kuling varchar2(100);
  v_wms_zone   varchar2(100);

begin

  v_date := sysdate;

  --查询SAP所有未完成的销售订单，插入未清订单表；
  insert into inv_wm_unclear_temp
    select a.vbeln as oem_code,
           a.posnr as oem_line,
           a.werks as sap_company_code,
           a.lgort as sap_loc_code,
           d.zone_code as zone_code,
           c.kunnr as bpp_code,
           case
             when a.erdat is not null then
              to_date(a.erdat, 'yyyymmdd')
             else
              v_date
           end as oem_date,
           c.bstnk as pi,
           a.matnr as sap_item,
           a.kwmeng as oem_qty,
           v_date
      from vbap@opple_sap a
      left join vbup@opple_sap b on a.vbeln = b.vbeln
                                and a.posnr = b.posnr
      left join vbak@opple_sap c on a.vbeln = c.vbeln
      left join base_sap_loc d on a.werks || a.lgort =
                                  d.source_company_code || d.sap_loc_code
                              and d.del_flag = '0'
     where a.lgort is not null
       and a.matnr is not null
       and a.werks in ('6000', '8000', '8100','7000')
       and GBSTA between 'A' and 'B'
       and AUART in ('ZOR1', 'ZFD')
       and VKORG in ('3002', '6002', '8102', '7001')
       and b.lfsta <> 'C';

  --遍历游标计算未清订单;
  open cur_wm_unclear;
  fetch cur_wm_unclear bulk collect
    into r_wm_unclear;
  close cur_wm_unclear;

  if r_wm_unclear.count > 0 then
    for i in r_wm_unclear.first .. r_wm_unclear.last loop
    
      select case
               when sum(rfmng) is null then
                0
               else
                sum(rfmng)
             end as ext_qty
        into v_ext_qty
        from vbfa@opple_sap
       where vbtyp_N = 'J'
         and vbelv = r_wm_unclear(i).oem_code
         and posnv = r_wm_unclear(i).oem_line;
    
      --修改临时表数据，减去已经交货的数量：
      update inv_wm_unclear_temp
         set oem_qty = r_wm_unclear(i).oem_qty - v_ext_qty
       where get_inv_time >= trunc(sysdate)
         and oem_code = r_wm_unclear(i)
      .oem_code
         and oem_line = r_wm_unclear(i)
      .oem_line
         and sap_item = r_wm_unclear(i).sap_item;
    
    end loop;
  end if;

  --把非法数据都删掉：
  delete from inv_wm_unclear_temp
   where get_inv_time >= trunc(sysdate)
     and (oem_qty <= 0 or sap_loc_code = ' ');

  commit;

  --把WMS外贸的库存按照库龄和客户汇总后插入库存临时表：
  insert into inv_wm_kucun
    select a.*, rownum as paixu
      from (select sys_office_code,
                   item_code,
                   max(aux2) as bpp_code,
                   zone_code,
                   floor(sysdate - to_date(batch, 'yymmdd')) as wms_kuling,
                   sum(qty) as stor_qty,
                   sysdate as get_inv_time
              from inv_stor
             where zone_code in ('WJWMWM', 'ZSWMWM')
               and status <> 'PICK'
             group by sys_office_code, item_code, zone_code, batch
             order by floor(sysdate - to_date(batch, 'yymmdd'))) a;

  --遍历游标2分摊WWM库龄库存：
  open cur_wm_kucun;
  fetch cur_wm_kucun bulk collect
    into r_wm_kucun;
  close cur_wm_kucun;

  if r_wm_kucun.count > 0 then
    for i in r_wm_kucun.first .. r_wm_kucun.last loop
    
      --把物料代码截取前面的0：
      if r_wm_kucun(i).sap_item like '%-%' then
        v_item_code := r_wm_kucun(i).sap_item;
      else
        v_item_code := r_wm_kucun(i).sap_item * 1 || '';
      end if;
    
      --把客户代码截取前面的0：
      v_bpp_code  := r_wm_kucun(i).bpp_code * 1 || '';
      v_zone_code := r_wm_kucun(i).zone_code;
    
      --查询此物料的行数：
      select count(1)
        into v_item_count
        from inv_wm_kucun
       where get_inv_time >= trunc(sysdate)
            --and zone_code = v_zone_code
         and item_code = v_item_code;
    
      --初始化几个库存值：
      v_add_qty  := 0;
      v_stor_qty := 0;
    
      while v_add_qty < r_wm_kucun(i).oem_qty and v_item_count > 0 loop
      
        --查询出本物料排序字段最大是多少：
        select max(paixu)
          into v_paixu_max
          from inv_wm_kucun
         where get_inv_time >= trunc(sysdate)
              --and zone_code = v_zone_code
           and item_code = v_item_code;
      
        --查询这条最大的数量是多少：
        select stor_qty, bpp_code, wms_kuling, zone_code
          into v_stor_qty, v_stor_bpp, v_wms_kuling, v_wms_zone
          from inv_wm_kucun
         where get_inv_time >= trunc(sysdate)
              --and zone_code = v_zone_code
           and item_code = v_item_code
           and paixu = v_paixu_max;
      
        --如果总数量加上之前累加的数量超过了SAP未清数，则把此次的数量算成剩下的数量
        if r_wm_kucun(i).oem_qty < v_add_qty + v_stor_qty then
          v_stor_qty := r_wm_kucun(i).oem_qty - v_add_qty;
        
          --把这行数据的数量减掉：
          update inv_wm_kucun
             SET stor_qty = stor_qty - v_stor_qty
           where get_inv_time >= trunc(sysdate)
                --and zone_code = v_zone_code
             and item_code = v_item_code
             and paixu = v_paixu_max;
        
        else
        
          --没超过未清数，把这个行数据删掉,并总数减1：
          delete from inv_wm_kucun
           where get_inv_time >= trunc(sysdate)
                --and zone_code = v_zone_code
             and item_code = v_item_code
             and paixu = v_paixu_max;
        
          v_item_count := v_item_count - 1;
        
        end if;
      
        --插入一行数据到结果表：
        insert into inv_wm_unclear
          (OEM_CODE,
           OEM_LINE,
           SAP_COMPANY_CODE,
           SAP_LOC_CODE,
           ZONE_CODE,
           BPP_CODE,
           OEM_DATE,
           PI,
           SAP_ITEM,
           ITEM_CODE,
           OEM_QTY,
           WMS_QTY,
           WMS_KULING,
           WMS_BPP_CODE,
           WMS_ZONE_CODE,
           GET_INV_TIME)
        VALUES
          (r_wm_kucun(i).oem_code,
           r_wm_kucun(i).oem_line,
           r_wm_kucun(i).sap_company_code,
           r_wm_kucun(i).sap_loc_code,
           r_wm_kucun(i).zone_code,
           r_wm_kucun(i).bpp_code,
           r_wm_kucun(i).oem_date,
           r_wm_kucun(i).pi,
           r_wm_kucun(i).sap_item,
           v_item_code,
           r_wm_kucun(i).oem_qty,
           v_stor_qty,
           v_wms_kuling,
           v_stor_bpp,
           v_wms_zone,
           v_date);
      
        --总数累加：
        v_add_qty := v_stor_qty + v_add_qty;
      
      end loop;
    
      if v_add_qty < r_wm_kucun(i).oem_qty and v_item_count = 0 then
      
        --插入一行数据到结果表：
        insert into inv_wm_unclear
          (OEM_CODE,
           OEM_LINE,
           SAP_COMPANY_CODE,
           SAP_LOC_CODE,
           ZONE_CODE,
           BPP_CODE,
           OEM_DATE,
           PI,
           SAP_ITEM,
           ITEM_CODE,
           OEM_QTY,
           WMS_QTY,
           WMS_KULING,
           WMS_BPP_CODE,
           WMS_ZONE_CODE,
           GET_INV_TIME)
        VALUES
          (r_wm_kucun(i).oem_code,
           r_wm_kucun(i).oem_line,
           r_wm_kucun(i).sap_company_code,
           r_wm_kucun(i).sap_loc_code,
           r_wm_kucun(i).zone_code,
           r_wm_kucun(i).bpp_code,
           r_wm_kucun(i).oem_date,
           r_wm_kucun(i).pi,
           r_wm_kucun(i).sap_item,
           v_item_code,
           r_wm_kucun(i).oem_qty,
           v_add_qty - r_wm_kucun(i).oem_qty,
           'WMS无库存',
           'WMS无库存',
           'WMS无库存',
           v_date);
      
      end if;
    
    end loop;
  end if;

  commit;

end sp_wm_sale_unclear;
