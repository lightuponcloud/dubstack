server {
    if ($host = example.com) {
        return 301 https://$host$request_uri;
    } # managed by Certbot


	listen 80;
	listen [::]:80;
	server_name example.com www.example.com;
	return 301 https://example.com$request_uri;
}

server {
	listen 443;
	listen [::]:443 ssl;
	server_name example.com www.example.com 

	ssl on;
	ssl_certificate /etc/letsencrypt/live/example.com/fullchain.pem; # managed by Certbot
	ssl_certificate_key /etc/letsencrypt/live/example.com/privkey.pem; # managed by Certbot

	ssl_session_timeout 5m;

	root /home/xentime/src/light/templates;
	index index.html;

	location / {
	    proxy_pass_header Authorization;
	    proxy_pass http://127.0.0.1:8884;
	    proxy_set_header Host $host;
	    proxy_set_header X-Real-IP $remote_addr;
	    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
	    proxy_http_version 1.1;
	    proxy_set_header Connection "";
	    proxy_buffering off;
	    client_max_body_size 0;
	    proxy_read_timeout 36000;
	    proxy_redirect off;
	}

	location /media {
	    alias /home/xentime/src/light/media;
	    autoindex off;
	}

	location /media/release {
	    alias /home/xentime/src/light/media/release;
	    autoindex on;
	    index update.xml;
	}

	set $riak_upstream 127.0.0.1:8081;
	set $bucket_upstream 127.0.0.1:8080;

	location /riak {
	    proxy_pass_header Authorization;
	    proxy_pass http://$riak_upstream;
	    proxy_set_header Host $host;
	    proxy_set_header Upgrade $http_upgrade;
	    proxy_set_header Connection "upgrade";
	    proxy_set_header X-Real-IP $remote_addr;
	    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
	    proxy_http_version 1.1;
	    proxy_buffering off;
	    client_max_body_size 0;
	    proxy_connect_timeout 300;
	    proxy_read_timeout 300;
	    proxy_send_timeout 300;
	    proxy_redirect off;
	}

	location /riak-media {
	    alias /home/xentime/src/middleware/priv;
	    autoindex off;
	}

	location /static {
	    alias /home/xentime/src/light/media;
	    autoindex off;
	}

	location /apple-touch-icon-120x120-precomposed.png {
		alias /home/xentime/src/light/media/x_logo.png;
	}

	location /apple-touch-icon-precomposed.png {
		alias /home/xentime/src/light/media/x_logo.png;
	}

	location /robots.txt {
		alias /home/xentime/src/xentime/media/robots.txt;
	}

	location /favicon.ico {
		alias /home/xentime/src/xentime/media/favicon.ico;
	}

	location ~ ^/sitemap.xml$ {
		rewrite ^ /media/sitemap.xml redirect;
	}
}
