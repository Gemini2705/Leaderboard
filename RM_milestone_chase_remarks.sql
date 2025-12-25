next_milestone_chase as(
	select
	*,
	CASE 
	    WHEN monthly_secured_files >= 4 AND monthly_disbursal_amount >= 3000000 THEN 'Platinum'
	    WHEN monthly_secured_files >= 3 AND monthly_disbursal_amount >= 2000000 THEN 'Gold'
	    WHEN monthly_secured_files >= 2 AND monthly_disbursal_amount >= 1500000 THEN 'Silver'
	    ELSE '-'
	END AS milestone_achieved,
	
	CASE 
	    WHEN monthly_secured_files >= 4 AND monthly_disbursal_amount >= 3000000 THEN Null
	    WHEN monthly_secured_files >= 3 AND monthly_disbursal_amount >= 2000000 THEN 'Platinum'
	    WHEN monthly_secured_files >= 2 AND monthly_disbursal_amount >= 1500000 THEN 'Gold'
	    ELSE 'Silver'
	END AS next_milestone_label
	
	from current_month_data
),

targets as (
	select
	*,
	
	case
		when next_milestone_label = 'Silver' and monthly_secured_files < 2 then (2-monthly_secured_files)
		when next_milestone_label = 'Gold' and monthly_secured_files < 3 then (3-monthly_secured_files)
		when next_milestone_label = 'Platinum' and monthly_secured_files < 4 then (4-monthly_secured_files)
		else 0
	end as secured_files_target,
	
	case
		when next_milestone_label = 'Silver' and monthly_disbursal_amount < 1500000 then (1500000-monthly_disbursal_amount)
		when next_milestone_label = 'Gold' and monthly_disbursal_amount < 2000000 then (2000000-monthly_disbursal_amount)
		when next_milestone_label = 'Platinum' and monthly_disbursal_amount < 3000000 then (3000000-monthly_disbursal_amount)
		else 0
	end as disbursal_amount_target
	
	from next_milestone_chase
)

select
*,

CASE
    WHEN milestone_achieved = '-' AND secured_files_target > 0 AND disbursal_amount_target > 0 THEN CONCAT('Get ', secured_files_target, ' more files disbursed & ₹', disbursal_amount_target, ' disbursal this month for Silver.')
    WHEN milestone_achieved = '-' AND secured_files_target > 0 AND disbursal_amount_target = 0 THEN CONCAT('Get ', secured_files_target, ' more files disbursed this month for Silver.')
    WHEN milestone_achieved = '-' AND secured_files_target = 0 AND disbursal_amount_target > 0 THEN CONCAT('Get ₹', disbursal_amount_target, ' disbursal this month for Silver.')
    WHEN secured_files_target > 0 AND disbursal_amount_target > 0 THEN CONCAT('Congrats on ', milestone_achieved, '! Get ', secured_files_target, ' more files disbursed & ₹', disbursal_amount_target, ' disbursal this month for ', next_milestone_label, '.')
    WHEN secured_files_target > 0 AND disbursal_amount_target = 0 THEN CONCAT('Congrats on ', milestone_achieved, '! Get ', secured_files_target, ' more files disbursed this month for ', next_milestone_label, '.')
    WHEN secured_files_target = 0 AND disbursal_amount_target > 0 THEN CONCAT('Congrats on ', milestone_achieved, '! Get ₹', disbursal_amount_target, ' disbursal this month for ', next_milestone_label, '.')
    ELSE 'Congrats on Platinum! You’ve reached the top milestone.'
END AS message_remarks

from targets
