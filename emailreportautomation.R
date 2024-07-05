setwd('/home/ubuntu/Prakhar')
library(readr)
library(data.table)
library('DBI')


library(PostgreSQL)
library(RPostgreSQL)
library('assertthat')
library('bigrquery')

library('httr')

library(rJava)

library(httr)
library(lubridate)

library(httpuv)
library(doBy)
library(RGA)
library('dplyr')



drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname="oyoprod",host="******",
                 port=5432,user="conversion",
                 password="******" )
#dbClearResult(dbListResults(con)[[1]])
ana <- dbConnect(drv, dbname="analytics",host="*******",
                 port=5432,user="conversion",
                 password="******")
ads_table_pre <- dbGetQuery(ana, paste0("select * from ads_table_pre"))
#head(ads_table_pre)

query <- "select had.hotel_id, h.oyo_id,h.name as oyo_name, a.id as agreement_id, a.default_rooms as contracted_rooms, had.valid_from, had.valid_till, ce.enum_val as contract_type, c.name as cluster, c.id as cluster_id,
ci.name as city, hu.name	 as hub, ce2.enum_val as status
from hotels as h
left join hotel_agreement_details as had
on h.id = had.hotel_id
left join agreements as a
on had.agreement_id = a.id
left join clusters as c
on h.cluster_id = c.id
left join cities as ci on
c.city_id = ci.id
left join hubs as hu
on ci.hub_id = hu.id
left join (select * from crs_enums where table_name = 'agreements' and column_name = 'agreement_type') as ce
on a.agreement_type = ce.enum_key
left join (select * from crs_enums where table_name = 'hotels' and column_name = 'status') as ce2
on h.status = ce2.enum_key
where  had.valid_from <= current_date and current_date <=valid_till and  h.status in (2) 
and a.agreement_type in (5,6,7) and h.id not in (9127, 5671,70)
"

smart_oyo_data <- dbGetQuery(con, query)
nrow(smart_oyo_data)
flagship_oyo_data <- dbGetQuery(con, paste0("select h.id as hotel_id, h.oyo_id, ct.name as city_name from hotels as h left join clusters as c on h.cluster_id = c.id left join cities as ct on c.city_id = ct.id where h.status in (2) and h.category = 5"))
oth_oyo_data <- dbGetQuery(con, paste0("select h.id as hotel_id, h.oyo_id from hotels as h where h.status in (2) and h.category = 5 and h.name ilike '%townhouse%'"))

frontier_properties <- dbGetQuery(con, paste0("select h.id as hotel_id, h.oyo_id from hotels as h left join taggings as tg on tg.taggable_id = h.id
                                              where tg.tag_name = 'frontier_communication' and h.status in (2)"))

flagship_oyo_data <- subset(flagship_oyo_data,!(flagship_oyo_data$hotel_id %in% frontier_properties$hotel_id))
smart_oyo_data <- subset(smart_oyo_data,!(smart_oyo_data$hotel_id %in% frontier_properties$hotel_id))
oth_oyo_data <- subset(oth_oyo_data,!(oth_oyo_data$hotel_id %in% frontier_properties$hotel_id))

df1 <- data.frame("oyo_id"=c(smart_oyo_data$oyo_id,flagship_oyo_data$oyo_id,oth_oyo_data$oyo_id,frontier_properties$oyo_id))
df1$hotel_cat <- ifelse(df1$oyo_id %in% smart_oyo_data$oyo_id,"Smart",
                        ifelse(df1$oyo_id %in% oth_oyo_data$oyo_id,"OTH",
                               ifelse(df1$oyo_id %in% flagship_oyo_data$oyo_id,"Flagship",
                                      ifelse(df1$oyo_id %in% frontier_properties$oyo_id,"Frontier","Marketplace"))))

df2 <- unique(df1[c('oyo_id','hotel_cat')])

all_live <- dbGetQuery(con, paste0('select h.oyo_id, h.id, city_name from
                                   (select id,oyo_id, cluster_id, city_id from hotels where status = 2) h
                                   left join 
                                   (select id, city_id from clusters) as c
                                   on h.cluster_id = c.id
                                   left join 
                                   (select id, name as city_name from cities) as ci on
                                   c.city_id = ci.id
                                   '))


hotel <- dbGetQuery(con, paste0('select id, oyo_id, name from hotels'))
market <- data.frame(all_live[!(all_live$oyo_id %in% df2$oyo_id),'oyo_id'])
market$hotel_cat <- 'Marketplace'
names(market)[1] <- 'oyo_id'

all_hotels <- rbind(df2, market)
all_hotels <- merge(x = all_hotels, y = hotel, by = 'oyo_id')
head(all_hotels)
nrow(all_hotels)
smart = all_hotels[all_hotels$hotel_cat == 'Smart',]
nrow(smart)

library(googlesheets)

s2<-gs_key('1u9cuVAYS8OmnBXM7_bR8JKFC2U2VWuwTDqdBmgCaXQo', lookup = NULL, visibility = NULL, verbose = TRUE)
c3 <- gs_read(ss = s2,ws='Property wise')
c3_1 <- c3[2:nrow(c3),c(1,3)]
c3_2 <- c3_1[which(c3_1$X3=='0'),1]

c3_hotels = data.frame(c3_2)

nrow(c3_hotels)
head(hotels)
names(c3_hotels) = 'oyo_id'
hotel <- dbGetQuery(con, paste0('select id, oyo_id, city from hotels'))

nrow(hotel)
head(c3_hotels)
colnames(smart)[3] = 'hotel_id'
head(smart_oyo_data)

c3_hotels <- merge(c3_hotels, hotel, by = 'oyo_id')
nrow(c3_hotels)

#head(c3_hotels)

#nrow(c3_hotels)
#head(c3_hotels)
#head(all_hotels)
#head(all_live)
#nrow(all_live)
#nrow(hotels)

#nrow(smart)
#head(smart_oyo_data)
#nrow(smart)
#nrow(smart_oyo_data)
head(c3_hotels)
#names(smart)[3] = 'hotel_id'
#shit = anti_join(smart_oyo_data,smart, by='hotel_id' )
#nrow(shit)head()
head(smart)
head(c3_hotels)
smart_c3_oyo_data= merge(smart, c3_hotels, by.x = 'hotel_id', by.y = 'id')
nrow(smart_c3_oyo_data)

#nrow(final_with_c3_smart)
head(all_live)
head(smart_c3_oyo_data)

nrow(smart_c3_oyo_data)

citynamec3smart = merge(smart_c3_oyo_data, all_live, by.x='hotel_id', by.y = 'id')
nrow(citynamec3smart)
head(citynamec3smart)
nrow(ads_table_pre)
head(all_live)
nrow(ads_table_pre)
nrow(hotel)
head(hotel)
ads <- merge(x = ads_table_pre, y = all_live,by.x = 'hotel_id', by.y = 'id')

head(ads)
final_ads <- ads %>% group_by(city_name) %>% summarize(n = n_distinct(hotel_id))
#write.csv(ads, file = "ads_check.csv", row.names = F)
#nrow(ads)
#ads= na.omit(ads)
city_c3 <- citynamec3smart %>% group_by(city_name) %>% summarize(n = n_distinct(hotel_id))
#nrow(city_c3)
#citynamec3smart
#nrow(city_c3)

#nrow(ads_table_pre)
#nrow(unique(ads_table_pre))
c3_ads <- merge(x = final_ads, y = city_c3,by = 'city_name')
colnames(c3_ads)[2] = 'total_eligible'
colnames(c3_ads)[3] = 'total_c3_smart'
write.csv(c3_ads, 'final_live_boostx.csv', row.names = F)





##################################

library(ggplot2)
library(rJava)
library(ggrepel)
library(formattable)
library(dplyr)
library(RPostgreSQL)
  

drv <- dbDriver("PostgreSQL")
dbDisconnect(con)
dbDisconnect(ana)
con <- dbConnect(drv, dbname="oyoprod",host="Prod-read-replica-analytics.czukitb4ckf9.ap-southeast-1.rds.amazonaws.com",
                 port=5432,user="conversion",
                 password="cONverSioN5610" )
ana <- dbConnect(drv, dbname="analytics",host="oyo-analytics.czukitb4ckf9.ap-southeast-1.rds.amazonaws.com",
                 port=5432,user="conversion",
                 password="COnvERSion829jsk7" )
ads_pre = dbGetQuery(ana, statement = paste("Select * from ads_table_pre"))

#head(ads_pre)


all_live <- dbGetQuery(con, paste0('select h.id as hotel_id, city_name from
                                   (select id,oyo_id, cluster_id, city_id from hotels where status = 2) h
                                   left join 
                                   (select id, city_id from clusters) as c
                                   on h.cluster_id = c.id
                                   left join 
                                   (select id, name as city_name from cities) as ci on
                                   c.city_id = ci.id
                                   '))
#str(all_live)

boost_services = dbGetQuery(con, statement = paste("select*from boost_services"))

#str(boost_services)
#nrow(all_live)
boost_city = merge(boost_services, all_live, by = 'hotel_id')
#boost_services[boost_city$city_name == 'Allahabad',]

june_boost_datas = data.frame()
june_boost = data.frame()
#rev_data = data.frame()

dates <- seq(as.Date("2018-07-01"), as.Date(Sys.Date()), by=1)
j=1
rev_data = data.frame() 
library(stringr)
for(i in dates){
  
  june_boost = subset(boost_city, start_date<=i & end_date>=i)
  june_boost$day = j
  x <- paste(june_boost$hotel_id, collapse = "','")
  y <- str_c("'", x, "'")
  
  data = dbGetQuery(con, statement = paste("select h.id as hotel_id,sum((coalesce(b.selling_amount,b.amount)-coalesce(b.discount,0))/(b.checkout-b.checkin)) as rev
                                           from bookings b
                                           inner join hotels h
                                           on h.id = b.hotel_id
                                           inner join clusters cl
                                           on cl.id = h.cluster_id
                                           inner join cities ct
                                           on ct.id = cl.city_id
                                           cross join calendar ca
                                           where ca.date>= b.checkin and ca.date< b.checkout and ca.date= '",as.Date(i, origin='1970-01-01'),"' and b.status in (1,2)  
                                           and h.id in (",y,")
                                           group by 1",sep=""))
  
  
  june_boost_data = merge(june_boost, data, by = 'hotel_id', all.x = T)
  june_boost_datas = rbind(june_boost_datas,june_boost_data)
  j=j+1
  
} 
#head(june_boost)
#nrow(june_boost) 
#nrow(june_boost_datas[june_boost_datas$day == 10,])

june_boost_datas$rev[is.na(june_boost_datas$rev) == T] = 0
boost_visible_citywise <- june_boost_datas %>% group_by(city_name) %>% summarize(n = n_distinct(hotel_id), rev = sum(rev))

june_boost_datas$take_incr = june_boost_datas$rev*june_boost_datas$revenue_percentage/100 
boost_visible_daywise <- june_boost_datas %>% group_by(day) %>% summarize(n = n_distinct(hotel_id), take_incr = sum(take_incr))

boost_visible_daywise = data.frame(boost_visible_daywise)
#head(boost_visible_daywise)
ggplot(data=boost_visible_daywise, aes(x=as.factor(day), y=n)) +
  geom_bar(stat="identity", fill="steelblue")+
  geom_text(aes(label=n), vjust=1.6, color="white", size=3.5)+
  theme_minimal()+labs(title="                         Day on Day no. of Properties Live", 
        x="Day", y = "No. of Properties")+
  ggsave("property.png", width = 10, height = 6, dpi = 70)

#head(boost_visible_daywise)
#str(boost_visible_daywise)
ggplot(data=boost_visible_daywise, aes(x=as.factor(day),y=take_incr/10^5)) +
  geom_line(size=1, color = "red", group = 1) +
  theme(axis.text=element_text(size=10))+
  labs(title="                                    Day on Day Absolute take (in Lacs)", 
        x="Day", y = "Absolute take in Lacs")+
  geom_text_repel(aes(label=round((take_incr/10^5), digits = 2),size=5))
  ggsave("takerate.png", width = 10, height = 6, dpi = 70)
  #signif(x, digits = 6)

#body=paste(body,"<hr>",'<img src="brns.png"><br>')
#write.csv(boost_visible_citywise, file = "boost_city_wise.csv", row.names = F)
#write.csv(boost_visible_daywise, file = "boost_day_wise.csv", row.names = F)

#hubs = dbGetQuery(con, statement = paste("Select*from hubs"))
#nrow(hubs)
#head(hubs)
query <- "select had.hotel_id, h.oyo_id,h.name as oyo_name, a.id as agreement_id, a.default_rooms as contracted_rooms, had.valid_from, had.valid_till, ce.enum_val as contract_type, c.name as cluster, c.id as cluster_id,
ci.name as city, hu.name	 as hub, ce2.enum_val as status
from hotels as h
left join hotel_agreement_details as had
on h.id = had.hotel_id
left join agreements as a
on had.agreement_id = a.id
left join clusters as c
on h.cluster_id = c.id
left join cities as ci on
c.city_id = ci.id
left join hubs as hu
on ci.hub_id = hu.id
left join (select * from crs_enums where table_name = 'agreements' and column_name = 'agreement_type') as ce
on a.agreement_type = ce.enum_key
left join (select * from crs_enums where table_name = 'hotels' and column_name = 'status') as ce2
on h.status = ce2.enum_key
where  had.valid_from <= current_date and current_date <=valid_till and  h.status in (2) 
and a.agreement_type in (5,6,7) and h.id not in (9127, 5671,70)
  "
head(smart_oyo_data)
smart_oyo_data <- dbGetQuery(con, query)

colnames(smart_oyo_data)
smart = smart_oyo_data[,c(1,2,11,12)]
head(smart)
nrow(smart)

smart_citywise = smart %>% group_by(city)%>% summarize(n = n_distinct(hotel_id))

smart_citywise = data.frame(smart_citywise)
head(smart_citywise)
#nrow(smart_citywise)
smart_city_hub = smart_oyo_data[,c(11,12)]
head(smart_city_hub)
smart_city_hub = unique(smart_city_hub)
#nrow(smart_city_hub)
smart_hub = merge(smart_citywise, smart_city_hub, by = 'city')
#nrow(smart_hub)
#smart_hub = unique(smart_hub)
colnames(smart_hub)[1] = c('city name')
head(smart_hub)

#write.csv(smart_hub, file = 'smartcitieshubname.csv', row.names = F)
final_live_boost = read.csv("final_live_boostx.csv", stringsAsFactors = F)
#head(final_live_boost)
#str(final_live_boost) 
#boost
boost_visible_citywise = data.frame(boost_visible_citywise)
#head(boost_visible_citywise)
city_wise = merge(boost_visible_citywise, final_live_boost, by = 'city_name')
#head(city_wise)
city_wise = city_wise[,c(1,2,4,5)]
#city_wise
names(city_wise) = c('city_name', 'total_opted', 'total_eligible', 'total_c30_smart')

head(city_wise)
city_wise$opt_percent = (city_wise$total_opted /city_wise$total_eligible)*100
colnames(smart_citywise)[1] = 'city_name'
city_wise1 = merge(city_wise, smart_citywise, by = 'city_name')
city_wise1 = city_wise1[,c(1,2,3,4,6,5)]
colnames(city_wise1)[5] = "total smart"

#head(city_wise1)
head(city_wise1)

#colnames(city_wise)
#head(smart_hub)
colnames(smart_hub)[1] = 'city_name'
hub_wise = merge(city_wise1,smart_hub, by = 'city_name')
#head(hub_wise)
colnames(hub_wise)
head(hub_wise)
#nrow(hub_wise)
hub_wise=hub_wise[,c(8,2,3,4,5)]
colnames(hub_wise)

#head(hub_wise)

hub_wise1 = aggregate(hub_wise[,c(2,3,4,5)],by=list(hub_wise$hub),FUN = sum)
head(hub_wise1)

colnames(hub_wise1)[1] = 'Hub'

hub_wise1$opt_percent = (hub_wise1$total_opted/hub_wise1$total_eligible )*100
head(hub_wise1)

#hub_wise1
#rev analysis



ads_pre = dbGetQuery(ana, statement = paste("Select * from ads_table_pre"))
#str(ads_pre)
#str(june_boost_datas)  
boost_services = dbGetQuery(con, statement = paste("select*from boost_services"))





#head(boost_services)
boost_active = subset(boost_services, status == 'active')
#nrow(boost_active)
#nrow(boost_services)
#length(unique(boost_services$hotel_id))
revenue = data.frame()
for(i in 1:nrow(boost_active)){
  dat = dbGetQuery(con, statement = paste("select h.id as hotel_id, sum((coalesce(b.selling_amount,b.amount)-coalesce(b.discount,0))/(b.checkout-b.checkin)) as rev
                                          from bookings b
                                          inner join hotels h
                                          on h.id = b.hotel_id
                                          inner join clusters cl
                                          on cl.id = h.cluster_id
                                          inner join cities ct
                                          on ct.id = cl.city_id
                                          cross join calendar ca
                                          where ca.date>= b.checkin and ca.date< b.checkout and ca.date>= '",as.Date(boost_active$start_date[i], origin='1970-01-01'),"' and ca.date<='",as.Date(ifelse(boost_active$end_date[i]<Sys.Date(), boost_active$end_date[i], Sys.Date()), origin='1970-01-01'),"' 
                                          and date(b.created_at + '5:30 hours') >= '",boost_active[i,'start_date'],"'
                                          and b.status in (1,2) and b.source in (17,21,12,23)
                                          and h.id = '",boost_active$hotel_id[i],"'
                                          group by 1",sep=""))
  
  
  
  
  revenue = rbind(revenue, dat)
}
summary(revenue$rev)
#head(ads_pre)
#nrow(ads_pre)
#head(revenue)
#nrow(revenue)

#colnames(revenue)[2] = 'start_date'
#head(revenue)

#head(boost_services)
#nrow(boost_services)
#table(boost_services$revenue_percentage)
boost_active$take_rate_increase = boost_active$revenue_percentage/100
nrow(boost_active)
boost_city = merge(boost_active, all_live, by = 'hotel_id')
nrow(boost_city)
head(boost_city)
colnames(boost_city)
boost_city = boost_city[,c(1:14,17,18)]
head(ads_pre)
head(boost_city)
#length(unique(ads_pre$hotel_id))
#length(unique(ads_pre$hotel_id,ads_pre$boost_factor))



rev_ana=merge(ads_pre, boost_city, by = c('hotel_id','take_rate_increase'))
nrow(boost_city)
nrow(rev_ana)

#colnames(rev_ana200)
#colnames(rev_ana2)


#nrow(rev_ana200)
#nrow(rev_ana2)
#nrow(boost_city2)

#boost_id = data.frame(boost_city$hotel_id)
#rev_ana_id = data.frame(rev_ana$hotel_id)
#nrow(rev_ana_id)
#head(all_live)

#colnames(rev_ana_id) = 'id'
#colnames(boost_id) = 'id'
#left_out = anti_join(boost_id, rev_ana_id)

#left_out



#rev_ana = rbind(rev_ana200, rev_ana2)
#colnames(rev_ana2)
#colnames(rev_ana200)
#rev_ana2 = rev_ana2[,c(1)]
#nrow(rev_ana)
#nrow(boost_services)
#boost_id = data.frame(boost_city$hotel_id)
#rev_ana_id = data.frame(rev_ana$hotel_id)
#nrow(rev_ana_id)
#head(all_live)

#colnames(rev_ana_id) = 'id'
#colnames(boost_id) = 'id'
#left_out = anti_join(boost_id, rev_ana_id)
#nrow(left_out)
#left_out
#head(boost_services)
#summary(boost_services$end_date)
#nrow(boost_services)
rev_analysis = merge(rev_ana, revenue, by = c('hotel_id'))
nrow(rev_ana)

nrow(rev_analysis)
nrow(revenue)
#nrow(rev_analysis)
head(rev_analysis)
colnames(rev_analysis)
#head(rev_analysis)
ncol(rev_analysis)
revenue_ana = rev_analysis[,c(1,2,6,7,8,12,15,16,26,27)]
nrow(revenue_ana)
head(revenue_ana)
str(revenue_ana)
revenue_ana$boosted_days = as.Date(ifelse(revenue_ana$end_date<Sys.Date(),revenue_ana$end_date, Sys.Date()), origin='1970-01-01')-revenue_ana$start_date
head(revenue_ana)
revenue_ana$boosted_days = as.numeric(revenue_ana$boosted_days)
revenue_ana$revenue_delta = as.numeric(revenue_ana$revenue_delta)
revenue_ana$current_revenue = as.numeric(revenue_ana$current_revenue)

colnames(revenue_ana)

#revenue_analysis = revenue_ana[,c(1,11,2,9,3,4,5,6,7,10,12,13,14)]
revenue_ana$revenue_before_boost_perday = revenue_ana$current_revenue/7
revenue_ana$revenue_promised_after_boost = revenue_ana$revenue_before_boost_perday*(1+revenue_ana$revenue_delta/100)
head(revenue_ana)
revenue_ana$revenue_after_boost_perday = revenue_ana$rev/(ifelse(revenue_ana$boosted_days>0,revenue_ana$boosted_days,1))

revenue_ana$delta_actual = ((revenue_ana$revenue_after_boost_perday- revenue_ana$revenue_before_boost_perday)/revenue_ana$revenue_before_boost_perday)*100
revenue_ana$delta_from_promised = ((revenue_ana$revenue_after_boost_perday- revenue_ana$revenue_promised_after_boost)/revenue_ana$revenue_promised_after_boost)*100
revenue_ana$percent_delivered = (revenue_ana$revenue_after_boost_perday/revenue_ana$revenue_promised_after_boost)*100
revenue_ana$delivery_status = ifelse(revenue_ana$percent_delivered>100,'overdelivered', ifelse(revenue_ana$percent_delivered>85 & revenue_ana$percent_delivered<100,'atleast 85% delivered', 'under delivered'))
table(revenue_ana$delivery_status)
analysis = data.frame(table(revenue_ana$delivery_status))
colnames(analysis) = c('delivery status', 'number')
number = sum(analysis$number)
tot = c('delivery status', number)
analysis = rbind(analysis,tot)
analysis$`delivery status` = as.character(analysis$`delivery status`)
analysis$`delivery status`[4] = 'Total'


body=paste("<hr>",'<p>Hi,<br>Below is the visibility of boost from 1st July, 2018 till date.</p>
           <p>1. Day on Day no. of Properties Live for Boost:<p><p><img src="property.png"><p/><br>')
body=paste(body,"<hr>",'<p>2. Day on Day Overall Absolute Take Increment (in Lacs) :<p/>
           <p><img src="takerate.png"><p/><br>')
body = paste(body,"<hr>",str_c("Total Absolute Take = ",as.character(round(sum(boost_visible_daywise$take_incr*(10^(-5))), digits = 2))," Lacs",'<br>'))
body=paste(body,"<hr>",'<p>3. City wise data for ongoing Boost:<p/><br>',formattable(city_wise1))

body=paste(body,"<hr>",'<p>4. Hub wise data for ongoing Boost:<p/><br>',formattable(hub_wise1))
body=paste(body,"<hr>",'<p>5. Delivery status for active properties:<p/><br>',formattable(analysis))


library(mailR)
library (syuzhet)

sender <- "conversionupdates@oyorooms.com" 

recipients = c('prakhar.prakash@.com','','')
subject<-paste("[",format(Sys.Date(), "%b-%d"),"] Boost Visibility [",nrow(june_boost)," live][Rs. ",as.character(round(sum(boost_visible_daywise$take_incr*(10^(-5))), digits = 2))," Lacs Take]",sep="")

send.mail(from = sender,
          to = recipients,
          subject=subject,
          body = body,
          smtp = list(host.name = "smtp.gmail.com", port = 465,
                      user.name="conversionupdates@oyorooms.com",
                      passwd="growth@conversion", ssl=TRUE),
          authenticate = TRUE,
          send = TRUE ,html = TRUE , inline = TRUE )





