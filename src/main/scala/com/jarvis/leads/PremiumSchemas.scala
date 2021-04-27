package com.jarvis.leads

object PremiumSchemas {
  case class Lead(
      lead_id: Int,
      marketing_rank: Int,
      gender: String,
      age: Int,
      credit_score: Int,
      urgency: String
  )
  case class Premium(
      lead_id: Int,
      marketing_rank: Int,
      gender: String,
      age: Int,
      credit_score: Int,
      urgency: String,
      urgency_immediately: Int,
      urgency_within_2_months: Int,
      urgency_not_sure: Int,
      expected_premium: Double
  )
}
