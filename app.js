// app.js
var express = require("express");
var pg = require('pg').native;
var logfmt = require("logfmt");
var http = require('http');
var https = require('https');
var yaml = require('js-yaml');

var app = express();
var server = http.createServer(app);
var logger = logfmt.namespace({app: "backchat"});
var database_url = process.env.BACKCHAT_DATABASE_URL;

app.use(logfmt.requestLogger());
app.use(express.bodyParser());

query = function(query) {
    pg.connect(database_url, function(err, client, done) {
	if(err) {
	    logger.log({message: "psql connection failed", db_url: database_url});
	    throw err;
	    return;
	}
	query(client, done);
    });
};

response = function(res, ok, code, data) {
    res.status(code).send({
	status: (ok ? 'ok' : 'error'),
	response: data
    });
};

error = function(res, code, msg) {
    response(res, false, code, msg);
};

ok_response = function(res, data) {
    response(res, true, 200, data);
};

ice = function(res) {
    error(res, 500, 'internal error');
};

handle_error = function(err, msg, err_callback) {
    if(err) {
	logger.log({error: err, message: msg});
	err_callback();
    }

    return err != null;	
}

USER_ROWS = "settings, first_name, last_name, (select SUM(purchases.clues) from purchases where purchases.user_id=$1) as bought_clues, (select COUNT(id) from clues where clues.revealed=true and clues.user_id=$1) as revealed_clues";

USER_DEFAULT_SETTINGS = {message_preview: false};
USER_DEFAULT_SETTINGS_STRING = "---\nmessage_preview: false\n"; //TODO fix this later; safeDump seems broken
USER_DEFAULT_CLUES = 3;

get_user = function(client, user_id, callback, error) {
    //{:user => {
    //    :new_user => !self.registered,
    //    :settings => self.settings,
    //    :available_clues => self.available_clues,
    //    :id => self.id,
    //    :full_name => self.name
    //  }

    client.query("SELECT " + USER_ROWS + " from users where id = $1", [user_id], function(err, result) {
	if(handle_error(err, "unable to query for user " + user_id, error))
	    return;

	if(result.rows.length != 1) {
	    logger.log({user_id: user_id, message: "Multiple rows"});
	    error();
	    return;
	}

	var user = result.rows[0];
	var settings = yaml.safeLoad(user.settings);

	var avail_clues = Math.max(0, user.bought_clues - user.revealed_clues);
	var json = {new_user: false,
		settings: settings,
		available_clues: avail_clues,
		id: user_id,
		full_name: user.first_name + " " + user.last_name
	       };

	callback(json);
    });
};

authenticate = function(client, provider, access_token, success, failure) {
    var insert_token = function(user) {
	client.query("insert into tokens (user_id, access_token, updated_at, created_at) values ($1, $2, now(), now())", 
		     [user.id, access_token], 
		     function(err, result) {
			 if(handle_error(err, "unable to insert token " + access_token + " for user " + user.id, failure))
			     return;
			 
			 //done!
			 success(user);
		     });
    };

    var search_and_authenticate = function(id_type, id, email) {
	logger.log({message: "searching", id_type: id_type, id: id, email: email});

	var update_user_id = function(user_id) {
	    logger.log({type: 'matched user', token: access_token, user_id: user_id, id_type: id_type, id_value: id, email: email});

	    get_user(client, user_id, function(user) {
		//TODO begin transaction
		email_to_set = user.email;
		if(email != null && email.length != 0) 
		    email_to_set = email;

		client.query("update users set " + id_type + " = $1, email = $2 where id = $3", 
			     [id, email_to_set, user_id], function(err, result) {
				 if(handle_error(err, "unable to set user for user_id " + id, failure))
				     return;

				 user.email = email_to_set;
				 user[id_type] = id;

				 insert_token(user);
			     });		
	    }, failure);
	};

	var query_for_user = function(column, value, failure_function) {
	    client.query("SELECT id from USERS where " + column + " = $1", [value], function(err, result) {
		if(handle_error(err, "unable to query for " + column + " " + value, failure)) 
		    return;
		
		if(result.rows.length == 1) {
		    update_user_id(result.rows[0].id);
		}
		else
		    failure_function();
	    });
	};

	query_for_user(id_type, id, function() {
	    query_for_user("email", email, function() {
		//create user
		client.query("insert into users (autocreated, registered, fake, featured, email, settings, created_at, updated_at, " + id_type + ") " +
			     "values (false, true, false, false, $1, $2, now(), now(), $3) returning id", [email, USER_DEFAULT_SETTINGS_STRING, id], 
			     function(err, result) {
				 if(handle_error(err, "unable to insert new user for " + id_type + " " + id, failure))
				     return;

				 user_id = result.rows[0].id;

				 logger.log({type: 'new user', token: access_token, user_id: user_id, id_type: id_type, id_value: id, email: email});

				 client.query("insert into purchases (user_id, clues, created_at, updated_at) values ($1, $2, now(), now())",
					      [user_id, USER_DEFAULT_CLUES], function(err, result) {
						  if(handle_error(err, "unable to insert default clues for user " + user_id, failure))
						      return;

						  user = {new_user: true,
							  settings: USER_DEFAULT_SETTINGS,
							  available_clues: USER_DEFAULT_CLUES,
							  id: user_id,
							  full_name: ""};
				 
						  insert_token(user);

					      });
			     });
	    });
	});
    };

    var parse_json = function(data, id_type, id_name) {
	var obj = null;
	try {
	    obj = JSON.parse(data);
	} catch(e) {
	    logger.log({exception: e, data: data, message: "not json " + id_type});
	    failure();
	    return;
	}

	if(obj[id_name] == null) {
	    logger.log({data: data, message: id_type + " no user id"});
	    failure();
	}
	else {
	    email = obj.email;
	    if(email == null) {
		email = ""; //TODO this matches what we are doing in ruby land, but I don't like it
	    }

	    search_and_authenticate(id_type, obj[id_name], email);
	}
    };

    var https_action = logger.time();    
    if(provider == 'facebook') {
	https.get({hostname: "graph.facebook.com",
		  path: '/me?access_token='+access_token
		  }, function(response) {
		      response.setEncoding('utf8');
		      var data = "";
		      response.on("data", function(chunk) {
			  data += chunk;
		      });
		      response.on("end", function() {
			  https_action.log({type: 'fb_get'});
			  parse_json(data, 'fb_id', 'id');			 
		      });
		  }).on('error', function(e) {
		      logger.log({error: e, message: "unable to query FB"});
		      failure();
		  });
    }
    else if(provider == 'gpp') {
	https.get({hostname: "www.googleapis.com",
		   path: '/oauth2/v1/tokeninfo?access_token='+access_token
		  }, function(response) {
		      response.setEncoding('utf8');
		      var data = "";
		      response.on("data", function(chunk) {
			  data += chunk;
		      });
		      response.on("end", function() {
			  https_action.log({type: 'gpp_get'});
			  parse_json(data, 'gpp_id', 'user_id');
		      });
		  }).on('error', function(e) {
		      logger.log({error: e, message: "unable to query gpp"});
		      failure();
		  });
    }
    else {
	https_action.log({type: 'invalid'});
	failure();	
    } 	
};

invalid_error = function(res, done) {
    return function() {
	error(res, 400, 'invalid access_token');
	if(done) 
	    done();
    };
};

app.post('/', function(req, res) {
    res.setHeader('Content-Type', 'application/json');

    if(req.body.access_token == null)  {
	error(res, 400, 'invalid access_token');
	return;
    }

    query(function(client, done) {
	//check to see if token exists
	var access_token = req.body.access_token;
	
	client.query("SELECT USER_ID FROM TOKENS WHERE ACCESS_TOKEN=$1", [access_token], function(err, result) {
	    if(handle_error(err, "unable to query for existing token", res, function() {
		ice(res);
		done();
	    }))
		return;

	    if(result.rows.length == 1) {
		get_user(client, result.rows[0].user_id, function(user) {
		    logger.log({type: 'existing token', token: access_token, user_id: user.id});
		    ok_response(res, {user: user});
		    done();
		}, invalid_error(res, done));
	    }
	    else {
		if(req.body.provider == null) {
		    error(res, 400, 'invalid provider');
		    done();
		    return;
		}

		authenticate(client, req.body.provider, 
			     access_token, function(user) {
				 ok_response(res, {user: user});
				 done();
			     },
			     invalid_error(res, done));
	    }
	});
    });
});

var port = Number(process.env.PORT || 5000);

app.listen(port, function() {
    logger.log({message: "listening on port " + port});
});
