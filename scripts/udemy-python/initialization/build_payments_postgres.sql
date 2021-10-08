drop table if exists temp_pay_check;
create temp table temp_pay_check as
	select
		b.member_id
		, b.charge_date
		, sum(c.charge_amount) as charge_to_point
	from
		(
		select distinct
			member_id
			, charge_date
		from credit.charge
		) b
		left join credit.charge c on b.member_id = c.member_id
			and c.charge_date <= b.charge_date
	group by b.member_id
		, b.charge_date			
;
drop table if exists temp_pay_check_2;
create temp table temp_pay_check_2 as
	select
		b.member_id
		, (random()*(b.charge_to_point-coalesce(bck.charge_to_point,0))+coalesce(bck.charge_to_point,0))::numeric(20,2) as paid_amt
		, case
			when b.charge_date+1 = f.charge_date
				then b.charge_date
			when f.charge_date is null and random() > .5 
				then least(b.charge_date+(random()*30)::int,current_date)
			else b.charge_date+(random()*(f.charge_date-b.charge_date))::int			
		end as pay_date 
	from
		(
		select *
			, row_number() over (partition by member_id order by coalesce(charge_date,'31Dec2999') asc) as r
		from temp_pay_check
		) b
		left join
			(
			select *
				, row_number() over (partition by member_id order by coalesce(charge_date,'31Dec2999') asc) as r
			from temp_pay_check
			) f on b.member_id = f.member_id
				and b.r+1 = f.r
		left join
			(
			select *
				, row_number() over (partition by member_id order by coalesce(charge_date,'31Dec2999') asc) as r
			from temp_pay_check
			) bck on b.member_id = bck.member_id
				and b.r-1 = bck.r
;	
drop table if exists credit.payment;
create table credit.payment as
	select
		b.member_id
		, b.pay_date
		, case
			when bck.paid_amt is null then b.paid_amt
			else b.paid_amt - bck.paid_amt
		end as paid_amount
	from
		(
		select *
			, row_number() over (partition by member_id order by coalesce(pay_date,'31Dec2999') asc) as r
		from temp_pay_check_2
		where pay_date is not null
	 	) b
		left join
			(
			select *
				, row_number() over (partition by member_id order by coalesce(pay_date,'31Dec2999') asc) as r
			from temp_pay_check_2
			where pay_date is not null
			) bck on b.member_id = bck.member_id
				and b.r-1 = bck.r
	where b.pay_date is not null
;
drop table if exists deliquents;
create temp table deliquents as
	select distinct member_id
	from credit.charge
	where random() < .1
;
delete from credit.payment
	where random() < .2
		and member_id in (select member_id from deliquents)
;