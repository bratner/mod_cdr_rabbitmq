/* 
 * FreeSWITCH Modular Media Switching Software Library / Soft-Switch Application
 * Copyright (C) 2005-2012, Anthony Minessale II <anthm@freeswitch.org>
 *
 * Version: MPL 1.1
 *
 * The contents of this file are subject to the Mozilla Public License Version
 * 1.1 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 * for the specific language governing rights and limitations under the
 * License.
 *
 * The Original Code is FreeSWITCH Modular Media Switching Software Library / Soft-Switch Application
 *
 * The Initial Developer of the Original Code is
 * Anthony Minessale II <anthm@freeswitch.org>
 * Portions created by the Initial Developer are Copyright (C)
 * the Initial Developer. All Rights Reserved.
 *
 * mod_cdr_rabbitmqr module developers:
 * Vadim Senderovich <daddyzgm@gmail.com>
 * Boris Ratner  <ratner2@gmail.com> 
 * 
 * made for http://commpeak.com
 * 
 * mod_cdr_rabbitmq.c -- Rabbitmq CDR Module
 *
 * Derived from:
 * mod_cdr_mongodb.c -- CDR Module to mongodb
 *
 */
#include <switch.h>
#include <amqp.h>
#include <amqp_framing.h>
#include <amqp_tcp_socket.h>
static struct {
	switch_memory_pool_t *pool;
	int shutdown;
	char *rabbitmq_host;
	int rabbitmq_port;
	char *rabbitmq_username;
	char *rabbitmq_password;
	char *rabbitmq_vhost;
	char *rabbitmq_exchange;
        char *rabbitmq_exchange_b;
	char *rabbitmq_routing_key;
	char *rabbitmq_routing_key_b;
	amqp_connection_state_t conn;
        amqp_socket_t* socket;
	switch_mutex_t *rabbitmq_mutex;
	switch_bool_t log_b;
	switch_bool_t encode_values;
} globals;

static switch_xml_config_item_t config_settings[] = {
	/* key, flags, ptr, default_value, syntax, helptext */
	SWITCH_CONFIG_ITEM_STRING_STRDUP("host", CONFIG_RELOADABLE, &globals.rabbitmq_host, "127.0.0.1", NULL, "Rabbitmq server host address"),
        SWITCH_CONFIG_ITEM("port", SWITCH_CONFIG_INT, CONFIG_RELOADABLE, &globals.rabbitmq_port, 5672, NULL, NULL, "Rabbitmq server TCP port"),

	SWITCH_CONFIG_ITEM_STRING_STRDUP("username", CONFIG_RELOADABLE, &globals.rabbitmq_username, NULL, NULL, "Rabbitmq server username"),
	SWITCH_CONFIG_ITEM_STRING_STRDUP("password", CONFIG_RELOADABLE, &globals.rabbitmq_password, NULL, NULL, "Rabbitmq server password"),

	SWITCH_CONFIG_ITEM_STRING_STRDUP("vhost", CONFIG_RELOADABLE, &globals.rabbitmq_vhost, NULL, NULL, "Rabbitmq server vhost"),
	SWITCH_CONFIG_ITEM_STRING_STRDUP("exchange", CONFIG_RELOADABLE, &globals.rabbitmq_exchange, NULL, NULL, "Rabbitmq server exchange"),
	SWITCH_CONFIG_ITEM_STRING_STRDUP("exchange_b", CONFIG_RELOADABLE, &globals.rabbitmq_exchange_b, NULL, NULL, "Rabbitmq server exchange for leg_b"),
	SWITCH_CONFIG_ITEM_STRING_STRDUP("routing_key", CONFIG_RELOADABLE, &globals.rabbitmq_routing_key, NULL, NULL, "Rabbitmq server routing key"),
        SWITCH_CONFIG_ITEM_STRING_STRDUP("routing_key_b", CONFIG_RELOADABLE, &globals.rabbitmq_routing_key_b, NULL, NULL, "Rabbitmq server routing key for leg_b"),
        SWITCH_CONFIG_ITEM("log-b-leg", SWITCH_CONFIG_BOOL, CONFIG_RELOADABLE, &globals.log_b, SWITCH_TRUE, NULL, NULL, "Log B-leg in addition to A-leg"),
        SWITCH_CONFIG_ITEM("encode", SWITCH_CONFIG_BOOL, CONFIG_RELOADABLE, &globals.encode_values, SWITCH_TRUE, NULL, NULL, "URL Encode values in JSON"),

	SWITCH_CONFIG_ITEM_END()
};


SWITCH_MODULE_LOAD_FUNCTION(mod_cdr_rabbitmq_load);
SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_cdr_rabbitmq_shutdown);
SWITCH_MODULE_DEFINITION(mod_cdr_rabbitmq, mod_cdr_rabbitmq_load, mod_cdr_rabbitmq_shutdown, NULL);

static switch_status_t check_amqp_error(amqp_rpc_reply_t x, char const *context) {
  switch (x.reply_type) {
    case AMQP_RESPONSE_NORMAL:
      return SWITCH_STATUS_SUCCESS;
      break;

    case AMQP_RESPONSE_NONE:
      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "%s: missing RPC reply type!\n", context);
      return SWITCH_STATUS_FALSE;
      break;

    case AMQP_RESPONSE_LIBRARY_EXCEPTION:
      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "%s: %s\n", context, amqp_error_string2(x.library_error));
      return SWITCH_STATUS_FALSE;
      break;

    case AMQP_RESPONSE_SERVER_EXCEPTION:
      switch (x.reply.id) {
        case AMQP_CONNECTION_CLOSE_METHOD: {
          amqp_connection_close_t *m = (amqp_connection_close_t *) x.reply.decoded;
	  switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "%s: server connection error %d, message: %.*s\n",
                  context,
                  m->reply_code,
                  (int) m->reply_text.len, (char *) m->reply_text.bytes);
	  return SWITCH_STATUS_FALSE;
          break;
        }
        case AMQP_CHANNEL_CLOSE_METHOD: {
          amqp_channel_close_t *m = (amqp_channel_close_t *) x.reply.decoded;
	  switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "%s: server channel error %d, message: %.*s\n",
                  context,
                  m->reply_code,
                  (int) m->reply_text.len, (char *) m->reply_text.bytes);
	  return SWITCH_STATUS_FALSE;
          break;
        }
        default:
	  switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "%s: unknown server error, method id 0x%08X\n", context, x.reply.id);
	  return SWITCH_STATUS_FALSE;
          break;
      }
      break;
  }

  return SWITCH_STATUS_FALSE;
}


static switch_status_t rabbitmq_publish_data(char *cdr, int leg_b)
{
	amqp_basic_properties_t props;
        char * exchange = globals.rabbitmq_exchange;
        char * routing_key = globals.rabbitmq_routing_key;
	
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Publishing leg %c CDR to rabbitmq. Total of %d bytes\n", leg_b?'b':'a', (int) strlen(cdr));

        /* use leg_a exchange and routing key for leg_b if not specified */
	if(leg_b && globals.rabbitmq_exchange_b)
            exchange = globals.rabbitmq_exchange_b;
        if(leg_b && globals.rabbitmq_routing_key_b)
            routing_key = globals.rabbitmq_routing_key_b;
	props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
	props.content_type = amqp_cstring_bytes("text/plain");
	props.delivery_mode = 1; 
    	if (0 != amqp_basic_publish(globals.conn, 
				1, 
				amqp_cstring_bytes(exchange),
  				amqp_cstring_bytes(routing_key),
  				0,
				0,
  				&props,
  				amqp_cstring_bytes(cdr))) {
								
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Publish failed\n");
		return SWITCH_STATUS_FALSE;
    	}
	
	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Publish succeeded\n");
	return SWITCH_STATUS_SUCCESS;
}

static switch_status_t connect_to_rabbitmq()
{
        int status;

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Connecting to Rabbitmq server %s:%d\n", globals.rabbitmq_host, globals.rabbitmq_port);
        /* if reconnecting then cleanup previous connection */
        if(globals.conn)
          amqp_destroy_connection(globals.conn);

        globals.conn = amqp_new_connection();
        if(!globals.conn){
              switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to create Rabbitmq connection\n");
              goto return_error;
        }
        globals.socket = amqp_tcp_socket_new(globals.conn);
        if(!globals.socket){
               switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to create Rabbitmq socket\n");
               amqp_destroy_connection(globals.conn);
               goto return_error;
        }
        status = amqp_socket_open(globals.socket, globals.rabbitmq_host, globals.rabbitmq_port);
        if(status){
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Failed to establish Rabbitmq connection\n");
            amqp_destroy_connection(globals.conn);
        } 

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Connection established, trying authentication\n");

        if (check_amqp_error(amqp_login(globals.conn, globals.rabbitmq_vhost, 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, globals.rabbitmq_username, globals.rabbitmq_password), "connect_to_rabbitmq") == SWITCH_STATUS_FALSE) {
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Rabbitmq Authentication failed\n");
                goto return_error;
        }
        amqp_channel_open(globals.conn, 1);

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Authenticated, openning channel\n");

        if (check_amqp_error(amqp_get_rpc_reply(globals.conn), "connect_to_rabbitmq") == SWITCH_STATUS_FALSE) {
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Rabbitmq openning channel failed\n");
                goto return_error;
        }

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Successfully connected to rabbitmq\n");
        return SWITCH_STATUS_SUCCESS;

	return_error:
                return SWITCH_STATUS_FALSE;
}

static switch_status_t my_on_reporting(switch_core_session_t *session)
{
        cJSON *json_cdr = NULL;
        char *json_text = NULL;
        switch_channel_t *channel = switch_core_session_get_channel(session);
        switch_status_t status = SWITCH_STATUS_FALSE;
        int is_b;

        if (globals.shutdown) {
                return SWITCH_STATUS_SUCCESS;
        }

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Starting on_reproting function, creating json\n");

        is_b = channel && switch_channel_get_originator_caller_profile(channel);
        if (!globals.log_b && is_b) {
                const char *force_cdr = switch_channel_get_variable(channel, SWITCH_FORCE_PROCESS_CDR_VARIABLE);
                if (!switch_true(force_cdr)) {
                        return SWITCH_STATUS_SUCCESS;
                }
        }

        if (switch_ivr_generate_json_cdr(session, &json_cdr, globals.encode_values?1:0) != SWITCH_STATUS_SUCCESS) {
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Error Generating JSON Data!\n");
                return SWITCH_STATUS_FALSE;
        }

        json_text = cJSON_PrintUnformatted(json_cdr);

        if (!json_text) {
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_CRIT, "Memory Error!\n");
                goto finish;
        }
	
	switch_mutex_lock(globals.rabbitmq_mutex);
	status = SWITCH_STATUS_SUCCESS;
	if (rabbitmq_publish_data(json_text, is_b) == SWITCH_STATUS_FALSE) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "Rabbitmq connection failed; attempting reconnect...\n");
		if (connect_to_rabbitmq() == SWITCH_STATUS_SUCCESS) {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "Rabbitmq connection re-established.\n");
			if (rabbitmq_publish_data(json_text, is_b) == SWITCH_STATUS_SUCCESS) {
				goto finish;
			}
		} else {
			switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Rabbitmq reconnect failed\n");
		}
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Rabbitmq failed to publish message\n");
	}
	
	finish:
		cJSON_Delete(json_cdr);
		switch_safe_free(json_text);
		switch_mutex_unlock(globals.rabbitmq_mutex);
		return status;
}


static switch_state_handler_table_t state_handlers = {
	/*.on_init */ NULL,
	/*.on_routing */ my_on_reporting,
	/*.on_execute */ NULL,
	/*.on_hangup */ NULL,
	/*.on_exchange_media */ NULL,
	/*.on_soft_execute */ NULL,
	/*.on_consume_media */ NULL,
	/*.on_hibernate */ NULL,
	/*.on_reset */ NULL,
	/*.on_park */ NULL,
	/*.on_reporting */ my_on_reporting
};


static switch_status_t load_config(switch_memory_pool_t *pool)
{
	switch_status_t status = SWITCH_STATUS_SUCCESS;

	if (switch_xml_config_parse_module_settings("cdr_rabbitmq.conf", SWITCH_FALSE, config_settings) != SWITCH_STATUS_SUCCESS) {
		return SWITCH_STATUS_FALSE;
	}

	return status;
}

SWITCH_MODULE_LOAD_FUNCTION(mod_cdr_rabbitmq_load)
{
	memset(&globals, 0, sizeof(globals));
	globals.pool = pool;
	if (load_config(pool) != SWITCH_STATUS_SUCCESS) {
		switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Unable to load or parse config!\n");
		return SWITCH_STATUS_FALSE;
	}

	if (connect_to_rabbitmq() == SWITCH_STATUS_FALSE) {
                return SWITCH_STATUS_FALSE;
        }

	switch_mutex_init(&globals.rabbitmq_mutex, SWITCH_MUTEX_NESTED, pool);	

	switch_core_add_state_handler(&state_handlers);
	*module_interface = switch_loadable_module_create_module_interface(pool, modname);
	
	return SWITCH_STATUS_SUCCESS;
}


SWITCH_MODULE_SHUTDOWN_FUNCTION(mod_cdr_rabbitmq_shutdown)
{
	globals.shutdown = 1;
	switch_core_remove_state_handler(&state_handlers);

    if (check_amqp_error(amqp_channel_close(globals.conn, 1, AMQP_REPLY_SUCCESS), "mod_cdr_rabbitmq_shutdown") == SWITCH_STATUS_FALSE) {
    	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Channel close error\n");
		goto return_success;
    }
    if (check_amqp_error(amqp_connection_close(globals.conn, AMQP_REPLY_SUCCESS), "mod_cdr_rabbitmq_shutdown") == SWITCH_STATUS_FALSE) {
    	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Connection close error\n");
		goto return_success;
    }
    if (amqp_destroy_connection(globals.conn) < 0) {
    	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "Connection destroy error\n");
		goto return_success;
    }

	return_success:
		return SWITCH_STATUS_SUCCESS;
}



/* For Emacs:
 * Local Variables:
 * mode:c
 * indent-tabs-mode:t
 * tab-width:4
 * c-basic-offset:4
 * End:
 * For VIM:
 * vim:set softtabstop=4 shiftwidth=4 tabstop=4:
 */
