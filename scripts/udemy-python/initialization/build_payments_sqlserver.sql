begin transaction trans;
drop table if exists #temp_pay_check;
	select
		b.member_id
		, b.charge_date
		, sum(c.charge_amount) as charge_to_point
	into #temp_pay_check
	from
		(
		select distinct
			member_id
			, charge_date
		from credit.dbo.charge
		) b
		left join credit.dbo.charge c on b.member_id = c.member_id
			and c.charge_date <= b.charge_date
	group by b.member_id
		, b.charge_date
;
drop table if exists #temp_pay_check_2;
	select
		b.member_id
		, rand()*(b.charge_to_point-coalesce(bck.charge_to_point,0))+coalesce(bck.charge_to_point,0) as paid_amt

		, case
			when dateadd(day,1,b.charge_date) = f.charge_date
				then b.charge_date
			when f.charge_date is null then
				case
					when rand() > .5 then
						case
							when dateadd(day,rand()*30,b.charge_date) > cast(getdate() as date)
								then cast(getdate() as date)
							else dateadd(day,rand()*30,b.charge_date)
						end
				end
			else dateadd(day,rand()*datediff(day,b.charge_date,f.charge_date),b.charge_date)
		end as pay_date
	into #temp_pay_check_2
	from
		(
		select *
			, row_number() over (partition by member_id order by coalesce(charge_date,'31Dec2999') asc) as r
		from #temp_pay_check
		) b
		left join
			(
			select *
				, row_number() over (partition by member_id order by coalesce(charge_date,'31Dec2999') asc) as r
			from #temp_pay_check
			) f on b.member_id = f.member_id
				and b.r+1 = f.r
		left join
			(
			select *
				, row_number() over (partition by member_id order by coalesce(charge_date,'31Dec2999') asc) as r
			from #temp_pay_check
			) bck on b.member_id = bck.member_id
				and b.r-1 = bck.r
;

drop table if exists credit.dbo.payment;
create table credit.dbo.payment
	(
	member_id int
	, pay_date date
	, paid_amount numeric(20,2)
	)
;

insert into credit.dbo.payment
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
			, row_number() over (partition by member_id order by pay_date asc) as r
		from #temp_pay_check_2
		where pay_date is not null
	 	) b
		left join
			(
			select *
				, row_number() over (partition by member_id order by pay_date asc) as r
			from #temp_pay_check_2
			where pay_date is not null
			) bck on b.member_id = bck.member_id
				and b.r-1 = bck.r
;

drop table if exists #deliquents;
	select distinct member_id
	into #deliquents
	from credit.dbo.charge
	where rand() < .1
;

delete from credit.dbo.payment
	where rand() < .2
		and member_id in (select member_id from #deliquents)
;
commit transaction trans;

--select * from credit.dbo.payment
