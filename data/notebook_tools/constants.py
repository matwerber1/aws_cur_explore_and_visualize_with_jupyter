ALL_POSSIBLE_CUR_COLUMNS = ['identity_line_item_id', 'identity_time_interval', 'bill_invoice_id', 'bill_invoicing_entity', 'bill_billing_entity', 'bill_bill_type', 'bill_payer_account_id', 'bill_billing_period_start_date', 'bill_billing_period_end_date', 'line_item_usage_account_id', 'line_item_line_item_type', 'line_item_usage_start_date', 'line_item_usage_end_date', 'line_item_product_code', 'line_item_usage_type', 'line_item_operation', 'line_item_availability_zone', 'line_item_resource_id', 'line_item_usage_amount', 'line_item_normalization_factor', 'line_item_normalized_usage_amount', 'line_item_currency_code', 'line_item_unblended_rate', 'line_item_unblended_cost', 'line_item_blended_rate', 'line_item_blended_cost', 'line_item_line_item_description', 'line_item_tax_type', 'line_item_legal_entity', 'product_product_name', 'product_purchase_option', 'product_size_flex', 'product_access_type', 'product_alarm_type', 'product_availability', 'product_availability_zone', 'product_backupservice', 'product_bundle', 'product_bundle_description', 'product_bundle_group', 'product_cache_engine', 'product_capacitystatus', 'product_classicnetworkingsupport', 'product_clock_speed', 'product_compute_family', 'product_cpu_architecture', 'product_cputype', 'product_current_generation', 'product_database_engine', 'product_datatransferout', 'product_dedicated_ebs_throughput', 'product_deployment_option', 'product_description', 'product_durability', 'product_ecu', 'product_endpoint_type', 'product_engine_code', 'product_enhanced_networking_supported', 'product_event_type', 'product_fee_code', 'product_fee_description', 'product_flow', 'product_free_trial', 'product_free_usage_included', 'product_from_location', 'product_from_location_type', 'product_from_region_code', 'product_gpu_memory', 'product_granularity', 'product_group', 'product_group_description', 'product_instance_family', 'product_instance_type', 'product_instance_type_family', 'product_intel_avx2_available', 'product_intel_avx_available', 'product_intel_turbo_available', 'product_invocation', 'product_license', 'product_license_model', 'product_location', 'product_location_type', 'product_logs_destination', 'product_marketoption', 'product_max_iops_burst_performance', 'product_max_iopsvolume', 'product_max_throughputvolume', 'product_max_volume_size', 'product_maximum_storage_volume', 'product_memory', 'product_memory_gib', 'product_memorytype', 'product_message_delivery_frequency', 'product_message_delivery_order', 'product_min_volume_size', 'product_minimum_storage_volume', 'product_network_performance', 'product_normalization_size_factor', 'product_operating_system', 'product_operation', 'product_physical_processor', 'product_platopricingtype', 'product_platostoragetype', 'product_platousagetype', 'product_pre_installed_sw', 'product_processor_architecture', 'product_processor_features', 'product_product_family', 'product_purchaseterm', 'product_queue_type', 'product_region', 'product_region_code', 'product_request_description', 'product_request_type', 'product_resource', 'product_resource_type', 'product_rootvolume', 'product_running_mode', 'product_scan_type', 'product_servicecode', 'product_servicename', 'product_sku', 'product_software_included', 'product_storage', 'product_storage_class', 'product_storage_family', 'product_storage_media', 'product_storage_type', 'product_tenancy', 'product_tickettype', 'product_time_window', 'product_to_location', 'product_to_location_type', 'product_to_region_code', 'product_transfer_type', 'product_usage_volume', 'product_usagetype', 'product_uservolume', 'product_vcpu', 'product_version', 'product_volume_api_name', 'product_volume_type', 'product_vpcnetworkingsupport', 'pricing_lease_contract_length', 'pricing_offering_class', 'pricing_purchase_option', 'pricing_rate_code', 'pricing_rate_id', 'pricing_currency', 'pricing_public_on_demand_cost', 'pricing_public_on_demand_rate', 'pricing_term', 'pricing_unit', 'reservation_amortized_upfront_cost_for_usage', 'reservation_amortized_upfront_fee_for_billing_period', 'reservation_effective_cost', 'reservation_end_time', 'reservation_modification_status', 'reservation_normalized_units_per_reservation', 'reservation_number_of_reservations', 'reservation_recurring_fee_for_usage', 'reservation_start_time', 'reservation_subscription_id', 'reservation_total_reserved_normalized_units', 'reservation_total_reserved_units', 'reservation_units_per_reservation', 'reservation_unused_amortized_upfront_fee_for_billing_period', 'reservation_unused_normalized_unit_quantity', 'reservation_unused_quantity', 'reservation_unused_recurring_fee', 'reservation_upfront_value', 'savings_plan_total_commitment_to_date', 'savings_plan_savings_plan_a_r_n', 'savings_plan_savings_plan_rate', 'savings_plan_used_commitment', 'savings_plan_savings_plan_effective_cost', 'savings_plan_amortized_upfront_commitment_for_billing_period', 'savings_plan_recurring_commitment_for_billing_period', 'savings_plan_start_time', 'savings_plan_end_time', 'savings_plan_offering_type', 'savings_plan_payment_option', 'savings_plan_purchase_term', 'savings_plan_region', 'split_line_item_reserved_usage', 'split_line_item_actual_usage', 'status']
"""List created based on AWS CUR from my account as of Nov 1 2023 with including of resource IDs and splitting of ECS detail by task ID enabled"""

# May be mistaken, but I seem to recall sorting was important for one of the pandas operations I was doing somewhere
ALL_POSSIBLE_CUR_COLUMNS.sort()