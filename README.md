# QingCloud-Marathon Connector

Connect [Mesosphere Marathon](https://mesosphere.github.io/marathon/) with [QingCloud load balancer](https://docs.qingcloud.com/guide/loadbalancer.html).

*Notice: this project has not been fully tested, do not use in production.*

## What can this connector do?

This connector implements [Service Discovery & Load Balancing](https://mesosphere.github.io/marathon/docs/service-discovery-load-balancing.html)
using QingCloud load balancer instead of self-managed HAProxy.

## How does it work?

This connector receives task update events using [Marathon Event Bus](https://mesosphere.github.io/marathon/docs/event-bus.html).
Then it adds or deletes backends of QingCloud load balancer using [QingCloud API](https://docs.qingcloud.com/api/index.html).

## Limitations currently

* Only one port supported for a Marathon app task
* Mesos slaves used by one app must be located in the same vxnet
* ...

## Requirements

```
$ pip install -r requirements.txt
```

## Quickstart

### Step 1, set your QingCloud API access key

Edit config file `config.py` and fill the following options:

```python
QY_ZONE_ID = ''
QY_ACCESS_KEY_ID = ''
QY_SECRET_ACCESS_KEY = ''
```

### Step 2, write web application definitions

Create a load balancer and add a empty listener for your application in QingCloud.
Write down the id of load balancer and listener.
Listener policy is supported, see more details below.

Fill the following JSON and save to `apps/you-app-name.json`.

```javascript
{
  "app_id": "Your Marathon app id",
  "vxnet_id": "The id of QingCloud vxnet where your Mesos slaves are located",
  "load_balancer_id": "The id of QingCloud load balancer",
  "listener_id": "The id of QingCloud load balancer listener",
  "weight": 5
}
```

Notice: DO NOT forget the leading slash of your Marathon app id.

### Step 3, launch the connector

```
$ python qingcloud-marathon.py
2015-11-17 00:58:10,371 - QingCloudClient - INFO - QingCloud initialized.
2015-11-17 00:58:10,371 - EventHandler - INFO - Starting event handler...
2015-11-17 00:58:10,371 - EventHandler - INFO - Event handler running...
2015-11-17 00:58:10,385 - werkzeug - INFO -  * Running on http://0.0.0.0:5000/ (Press CTRL+C to quit)
```

Now the connector is listening on `0.0.0.0:5000`.

### Step 4, register event subscriber in Marathon

```
$ echo "http_callback" > /etc/marathon/conf/event_subscriber
$ echo "http://HOSTNAME:5000/" > /etc/marathon/conf/http_endpoints
```

Replace the `HOSTNAME` by the host connector running on.

Don't forget to restart Marathon: `$ systemctl restart marathon`

### Step 5, scale your app in Marathon and check the result in QingCloud console

## Configuration

Configurations will be read from `config.py` by default, or from the file `CONF` env point to if set.

You can find all configurations in `config.py`.

## App Definition

```javascript
{
  "app_id": "Your Marathon app id",
  "vxnet_id": "The id of QingCloud vxnet where your Mesos slaves are located",
  "load_balancer_id": "The id of QingCloud load balancer",
  "listener_id": "The id of QingCloud load balancer listener",
  "policy_id": "Optional. The id of QingCloud load balancer listener policy id",
  "weight": 5
}
```

App definitions are loaded from `./apps` folder. You can override the path by setting `APPS_DIR` env.

## TODO

* TESTING! TESTING! TESTING!
* Support Mesos slaves located in different vxnets(still in one router)
* Check and rebuild all QingCloud backends on first running or on crash
* Scalability and High Availability
* ...

## License

The MIT License (MIT)

Copyright (c) 2015 Microdog <kainan.zhu@outlook.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
