---- CONFIDENTIAL (only for Personal Reference)
/*+ETLM
{
  depend:{
    replace:[
      {
        name:"looker.master_houdini"
      },
      {
        name:"looker.d_distributor_order_items"
      },
      {
        name:"looker.d_distributor_shipment_items"
      },
      {
        name:"looker.fwi_vendor_mapping"
      },
      {
        name:"looker.f3_asin_list"
      },
      {
        name:"looker.pn_daily_nyr"
      },
      {
        name:"ISVS_DDL.O_APPOINTMENT_DETAILS"
      },
      {
        name:"ISVS_DDL.O_APPOINTMENT_SHIPITEMS"
      },
      {
        name:"ISVS_DDL.O_APPT_SHIPITEM_PO"
      }
    ]
  }
} */

with nyr as (

select
p.Order_Id,
max(p.nyr_qty) as nyr_qty,
min(p.dock_receive_datetime) as dock_receive_datetime

from houdinianalytics.looker.pn_daily_nyr p

group by
p.Order_Id

),
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
item as ( 

select 
distinct j.order_id,
max(k.f3_vertical) as f3_vertical

from looker.f3_asin_list k

left join houdinianalytics.looker.d_distributor_order_items j
on j.isbn = k.asin

and k.marketplace_id=1338980

group by
j.order_id

),
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PO as (

select
t.distributor_id,
t.order_id,
t.condition,
t.warehouse_id,
t.order_datetime,
nvl(t.earliest_vendor_ship_datetime,t.earliest_vendor_delivery_date) as EVSD,
nvl(t.latest_vendor_ship_datetime,t.latest_vendor_delivery_date) as LVSD,
sum(t.quantity_submitted)as Quan_Sub,
sum(t.quantity_ordered) as Quan_Con,
sum(t.voided_quantity)as Quan_Voi

FROM houdinianalytics.looker.d_distributor_order_items t

where t.region_id = 1

and t.condition < 3
and t.warehouse_id in ('UAZ1','UCA6','UCA9','UCO1','UNV1','UIL1','UIN1','UMA3','UMN1','UOH1','UOH2','UTN1','UWI1','UFL2','UFL3','UFL4',
                            'UTX2','UTX3','UTX4','UTX5','UTX7','UVA1','UVA2','UCA1','UCA7','UCA8','UCA2','UCA3','UCA4','UCA5','JFK2','UGA2',
                                'UMD1','UNC1','UNC2','UNY1','UVA3','UGA3','UOR1','UWA2','UWA4','UWA1','HCA6','HIL2','HTX2','HWA3')
and 
(
       (      
              t.order_day between 
              (Select max(order_day) from houdinianalytics.looker.d_distributor_order_items where to_char(order_day,'fmday')='sunday')-126
                     and 
              (Select max(order_day) from houdinianalytics.looker.d_distributor_order_items where to_char(order_day,'fmday')='sunday')-1
       )

)

group by
t.distributor_id,
t.order_id,
t.condition,
t.warehouse_id,
t.order_datetime,
nvl(t.earliest_vendor_ship_datetime,t.earliest_vendor_delivery_date),
nvl(t.latest_vendor_ship_datetime,t.latest_vendor_delivery_date)

having sum(t.quantity_submitted) + sum(t.quantity_ordered) >= 1

),
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
delivery as (
select 
a.order_id,
a.warehouse_id,
min(a.received_datetime)as received_datetime,
sum(a.quantity_unpacked) as Quan_Rec

FROM houdinianalytics.looker.d_distributor_shipment_items a
where a.region_id = 1
and a.warehouse_id in ('UAZ1','UCA6','UCA9','UCO1','UNV1','UIL1','UIN1','UMA3','UMN1','UOH1','UOH2','UTN1','UWI1','UFL2','UFL3','UFL4',
                        'UTX2','UTX3','UTX4','UTX5','UTX7','UVA1','UVA2','UCA1','UCA7','UCA8','UCA2','UCA3','UCA4','UCA5','JFK2','UGA2',
                         'UMD1','UNC1','UNC2','UNY1','UVA3','UGA3','UOR1','UWA2','UWA4','UWA1','HCA6','HIL2','HTX2','HWA3')
                                  
group by
a.order_id,
a.warehouse_id
),

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
app as (

Select
po_id,
shipitem_creation_day,
creation_datetime,
last_updated_datetime,
last_updated_by,
appointment_type,
status,
requested_delivery_datetime,
carrier_id,
scheduled_arrival_datetime,
arrival_datetime,
scheduled_checkin_datetime,
checkin_datetime,
scheduled_checkout_datetime,
checkout_datetime,
closed_datetime,
freight_type,
freight_term,
appointment_units,
shipment_units

from(
select 
ISVS_DDL.O_APPT_SHIPITEM_PO.po_id,
ISVS_DDL.O_APPT_SHIPITEM_PO.shipitem_creation_day,
ISVS_DDL.O_APPOINTMENT_DETAILS.creation_datetime,
ISVS_DDL.O_APPOINTMENT_DETAILS.last_updated_datetime,
ISVS_DDL.O_APPOINTMENT_DETAILS.last_updated_by,
ISVS_DDL.O_APPOINTMENT_DETAILS.appointment_type,
ISVS_DDL.O_APPOINTMENT_DETAILS.status,
ISVS_DDL.O_APPOINTMENT_DETAILS.requested_delivery_datetime,
ISVS_DDL.O_APPOINTMENT_DETAILS.carrier_id,
ISVS_DDL.O_APPOINTMENT_DETAILS.scheduled_arrival_datetime,
ISVS_DDL.O_APPOINTMENT_DETAILS.arrival_datetime,
ISVS_DDL.O_APPOINTMENT_DETAILS.scheduled_checkin_datetime,
ISVS_DDL.O_APPOINTMENT_DETAILS.checkin_datetime,
ISVS_DDL.O_APPOINTMENT_DETAILS.scheduled_checkout_datetime,
ISVS_DDL.O_APPOINTMENT_DETAILS.checkout_datetime,
ISVS_DDL.O_APPOINTMENT_DETAILS.closed_datetime,
ISVS_DDL.O_APPOINTMENT_DETAILS.freight_type,
ISVS_DDL.O_APPOINTMENT_DETAILS.freight_term,
ISVS_DDL.O_APPOINTMENT_DETAILS.appointment_units,
ISVS_DDL.O_APPOINTMENT_SHIPITEMS.shipment_units,
rank() over (partition by po_id order by ISVS_DDL.O_APPOINTMENT_DETAILS.last_updated_datetime desc) as rankk


from ISVS_DDL.O_APPT_SHIPITEM_PO 

join ISVS_DDL.O_APPOINTMENT_SHIPITEMS 
on ISVS_DDL.O_APPOINTMENT_SHIPITEMS.shipitem_id = ISVS_DDL.O_APPT_SHIPITEM_PO.shipitem_id

join ISVS_DDL.O_APPOINTMENT_DETAILS 
on ISVS_DDL.O_APPOINTMENT_DETAILS.appointment_id = ISVS_DDL.O_APPOINTMENT_SHIPITEMS.appointment_id

where po_id in (
with nest_po as (

select
x.distributor_id,
x.order_id,
x.condition,
x.warehouse_id,
x.order_datetime,
nvl(x.earliest_vendor_ship_datetime,x.earliest_vendor_delivery_date) as EVSD,
nvl(x.latest_vendor_ship_datetime,x.latest_vendor_delivery_date) as LVSD,
sum(x.quantity_submitted)as Quan_Sub,
sum(x.quantity_ordered) as Quan_Con,
sum(x.voided_quantity)as Quan_Voi

FROM houdinianalytics.looker.d_distributor_order_items x

where x.region_id = 1

and x.condition < 3
and x.warehouse_id in ('UAZ1','UCA6','UCA9','UCO1','UNV1','UIL1','UIN1','UMA3','UMN1','UOH1','UOH2','UTN1','UWI1','UFL2','UFL3','UFL4',
                            'UTX2','UTX3','UTX4','UTX5','UTX7','UVA1','UVA2','UCA1','UCA7','UCA8','UCA2','UCA3','UCA4','UCA5','JFK2','UGA2',
                                'UMD1','UNC1','UNC2','UNY1','UVA3','UGA3','UOR1','UWA2','UWA4','UWA1','HCA6','HIL2','HTX2','HWA3')
and 
(
       (      
              x.order_day between 
              (Select max(order_day) from houdinianalytics.looker.d_distributor_order_items where to_char(order_day,'fmday')='sunday')-126
                     and 
              (Select max(order_day) from houdinianalytics.looker.d_distributor_order_items where to_char(order_day,'fmday')='sunday')-1
       )

)

group by
x.distributor_id,
x.order_id,
x.condition,
x.warehouse_id,
x.order_datetime,
nvl(x.earliest_vendor_ship_datetime,x.earliest_vendor_delivery_date),
nvl(x.latest_vendor_ship_datetime,x.latest_vendor_delivery_date)

having sum(x.quantity_submitted) + sum(x.quantity_ordered) >= 1

)

----------------------------------------
select distinct order_id from nest_po

)

group by 
ISVS_DDL.O_APPT_SHIPITEM_PO.po_id,
ISVS_DDL.O_APPT_SHIPITEM_PO.shipitem_creation_day,
ISVS_DDL.O_APPOINTMENT_DETAILS.creation_datetime,
ISVS_DDL.O_APPOINTMENT_DETAILS.last_updated_datetime,
ISVS_DDL.O_APPOINTMENT_DETAILS.last_updated_by,
ISVS_DDL.O_APPOINTMENT_DETAILS.appointment_type,
ISVS_DDL.O_APPOINTMENT_DETAILS.status,
ISVS_DDL.O_APPOINTMENT_DETAILS.requested_delivery_datetime,
ISVS_DDL.O_APPOINTMENT_DETAILS.carrier_id,
ISVS_DDL.O_APPOINTMENT_DETAILS.scheduled_arrival_datetime,
ISVS_DDL.O_APPOINTMENT_DETAILS.arrival_datetime,
ISVS_DDL.O_APPOINTMENT_DETAILS.scheduled_checkin_datetime,
ISVS_DDL.O_APPOINTMENT_DETAILS.checkin_datetime,
ISVS_DDL.O_APPOINTMENT_DETAILS.scheduled_checkout_datetime,
ISVS_DDL.O_APPOINTMENT_DETAILS.checkout_datetime,
ISVS_DDL.O_APPOINTMENT_DETAILS.closed_datetime,
ISVS_DDL.O_APPOINTMENT_DETAILS.freight_type,
ISVS_DDL.O_APPOINTMENT_DETAILS.freight_term,
ISVS_DDL.O_APPOINTMENT_DETAILS.appointment_units,
ISVS_DDL.O_APPOINTMENT_SHIPITEMS.shipment_units

order by
ISVS_DDL.O_APPOINTMENT_DETAILS.last_updated_datetime desc
)
where rankk=1

),
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
vendor as (
SELECT
looker.fwi_vendor_mapping.vendor_code, 
looker.fwi_vendor_mapping.vendor_name, 
looker.fwi_vendor_mapping.vendor_type

FROM looker.fwi_vendor_mapping

where looker.fwi_vendor_mapping.marketplace_id = 1338980 

),

----------------------------------------------------------------------------------------------------------------------------------------------------------------------

summary as (

select 
PO.order_id,
item.f3_vertical,
PO.distributor_id,
vendor.vendor_name,
PO.condition,
app.last_updated_by as app_last_updated_by,
app.appointment_type,
app.status,
app.carrier_id,
app.freight_type,
app.freight_term,
PO.warehouse_id,
PO.order_datetime,
PO.EVSD,
app.shipitem_creation_day,
app.requested_delivery_datetime,
app.creation_datetime,
app.last_updated_datetime,    
app.scheduled_arrival_datetime,
app.arrival_datetime,
app.scheduled_checkin_datetime,
app.checkin_datetime,
app.scheduled_checkout_datetime,
app.checkout_datetime,
app.closed_datetime,
nyr.dock_receive_datetime,
delivery.received_datetime,
PO.LVSD,
PO.Quan_Sub,
PO.Quan_Con,
PO.Quan_Voi,
delivery.Quan_Rec,
nyr.nyr_qty,
app.appointment_units,
app.shipment_units

from PO

left join delivery 
on delivery.order_id = po.order_id

left join nyr 
on nyr.order_id = po.order_id

left join vendor
on vendor.vendor_code = po.distributor_id

left join item
on item.order_id = po.order_id

left join app
on app.po_id = po.order_id

)

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


select * from summary
