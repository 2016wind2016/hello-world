/**
 * 实时分数排行榜-值越大名次越高
 * Created by wangkm on 2019-8-30.
 * Email: 417079820@qq.com.
 */
const assert = require('assert').strict;
const Log = require('../../../../lib/log');
const RTLeaderboard = require('./RTLeaderboard');

class RTScoreLeaderboard extends RTLeaderboard {
  constructor({conn, maxNum, keyRank, keyData}) {
    super({conn, maxNum, keyRank, keyData});
  }

  async rank(id, keyRank = this.keyRank(), keyData = this.keyData()) {
    assert.ok(id, `${this.name} rank id:${id}`);
    let rank = await this._conn.zrevrank(keyRank, id);
    if (rank === null) {
      return -1;
    }
    rank += 1;
    if (rank > this.maxNum) {
      rank = -1;
      this._conn.hdel(keyData, id);
      this._conn.zremrangebyrank(keyRank, 0, -this.maxNum);
    }
    return rank;
  }

  async range(start, stop, keyRank = this.keyRank(), keyData = this.keyData()) {
    assert.ok(start !== undefined && stop !== undefined, `${this.name} range start:${start} stop:${stop}`);
    let list = [];
    let ids = await this._conn.zrevrange(keyRank, start, stop);
    let reply = await this._conn.hmget(keyData, ids);
    for (let id of ids) {
      if (!reply[id]) {
        Log.error(`keyRank : ${keyRank}, id: ${id}, cannot find`);
        this.remove(id);
        continue;
      }
      let aMatchData = JSON.parse(reply[id]);
      list.push({id, rank: list.length + 1, aMatchData});
    }
    return list;
  }
}

module.exports = RTScoreLeaderboard;