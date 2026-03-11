# encoding:utf-8
import re
import requests
import json

# from lib.logger import log_info


class TalkDing:
    def __init__(self, token):
        self.token = token
    def send(self,content,title="通知"):
        head = {'Content-Type': 'application/json;charset=utf-8'}
        url = f'https://oapi.dingtalk.com/robot/send?access_token={self.token}'
        json_text = {
            "msgtype": "markdown",
            "markdown": {
                "title": title,
                "text": content
            },
            "at": {
                "atMobiles": [],
                "isAtAll": False
            }
        }

        requests.post(url, json.dumps(json_text), headers=head)


if __name__ == "__main__":
    # 5aa597436d1ea741e0eea74e568ac34a23558e9ba7b2ae71dfeb095b14485c1b
    talk = TalkDing("5aa597436d1ea741e0eea74e568ac34a23558e9ba7b2ae71dfeb095b14485c1b")
    talk.send("你好呀",title="测试通知")