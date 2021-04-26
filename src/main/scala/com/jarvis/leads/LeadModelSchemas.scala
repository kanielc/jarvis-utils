package com.jarvis.leads

object LeadModelSchemas {
  case class Lead(lead_id: Int, marketing_rank: Int, gender: String, age: Int, credit_score: Int, urgency: String)
}
