
select
  uid as trustedshops_answer_id,
  controller as controller_id,
  kunde_id as user_id,
  kundenaction_id as customer_action_id,
  mark as trustedshops_answer_score,
  orderReference as trustedshops_order_reference,
  comment as trustedshops_answer_comment,
  creationDate as created_at

from `planar-depth-242012.backend_data.backend__kundenbewertungen`
