/**
 * 虚基类-基于Redis-实时排行榜
 * TODO 时间排序逻辑待补充
 * Created by wangkm on 2019-8-30.
 * Email: 417079820@qq.com.
 */
const assert = require('assert').strict;
const _ = require('lodash');
const Log = require('../../../../lib/log');

class RTLeaderboard {
  constructor({conn, maxNum = 100, keyRank = 'rank', keyData = 'data', keyAll = 'all'}) {
    this._conn = conn; // redis conn
    this._maxNum = maxNum; // 榜单长度
    this._keyRank = keyRank; // 有序集合Key
    this._keyData = keyData; // 榜单玩家数据Key
    this._keyAll = keyAll; // 榜单保存Key
    if (new.target.name === 'RTLeaderboard') {
      throw new Error('RTLeaderboard is virtual class. You must inherit it!');
    }
    this.afterConstructor();
  }

  /**
   * @description 生成比赛数据
   * @param {string} id
   * @param {number} score
   * @param {object} imageData
   * @return {{score: *, imageData: *, id: *}}
   */
  createMatchData(id, score, imageData) {
    assert.ok(id && score && imageData, `${this.name} createMatchData id:${id}, score:${score}, imageData:${imageData}`);
    return {id, score, imageData};
  }

  get maxNum() {
    return this._maxNum;
  }

  keyRank(keyRank = this._keyRank) {
    return `${this.name}:${keyRank}`;
  }

  keyData(keyData = this._keyData) {
    return `${this.name}:${keyData}`;
  }

  keyAll(keyAll = this._keyAll) {
    return `${this.name}:${keyAll}`;
  }

  afterConstructor() {
    assert.ok(this.maxNum && this.keyRank() && this.keyData() && this.keyAll(), `${this.name} maxNum:${this.maxNum} keyRank:${this.keyRank()}, keyData:${this.keyData()}, keyAll:${this.keyAll()}`);
  }

  /**
   * @description 设置指定玩家排行榜数据
   * @param {object} aMatchData
   * @param {string} keyRank
   * @param {string} keyData
   * @return {Promise<*>}
   */
  async set(aMatchData, keyRank = this.keyRank(), keyData = this.keyData()) {
    Log.error(`${this.name}, aMatchData: ${JSON.stringify(aMatchData)}`);
    let id = aMatchData.id;
    let score = aMatchData.score;
    let imageData = aMatchData.imageData;
    assert.ok(id && score && imageData, `${this.name} save aMatchData:${aMatchData}`);
    const pipeline = this._conn.pipeline();
    pipeline.zadd(keyRank, score, id);
    pipeline.hset(keyData, id, JSON.stringify(imageData));
    return pipeline.exec();
  }

  /**
   * @description 取得指定玩家排行榜数据
   * @param {string} id
   * @param {string} keyRank
   * @param {string} keyData
   * @return {Promise<null|{rank: void, aMatchData: any, id: *}>}
   */
  async get(id, keyRank = this.keyRank(), keyData = this.keyData()) {
    assert.ok(id, `${this.name} get id:${id}`);
    let rank = this.rank(id, keyRank, keyData);
    if (rank <= 0) {
      return null;
    }
    let reply = await this._conn.hget(keyData, id);
    if (_.isEmpty(reply)) {
      return null;
    }
    let aMatchData = JSON.parse(reply);
    return {id, rank, aMatchData};
  }

  /**
   * @description 删除指定玩家排行榜数据
   * @param {string} id
   * @param {string} keyRank
   * @param {string} keyData
   * @return {*}
   */
  delete(id, keyRank = this.keyRank(), keyData = this.keyData()) {
    assert.ok(id, `${this.name} remove id:${id}`);
    const pipeline = this._conn.pipeline();
    pipeline.zrem(keyRank, id);
    pipeline.hdel(keyData, id);
    return pipeline.exec();
  }

  /**
   * @description 取得排名-纯虚函数
   * @param {string} id
   * @param {string} [keyRank]
   */
  rank(id, keyRank = this.keyRank(), keyData = this.keyData()) {
    throw new Error('RTLeaderboard is virtual func. You must override this!');
  }

  /**
   * @description 取得区间排名数据-纯虚函数
   * @param {number} start
   * @param {number} stop
   * @param {string} [keyRank]
   * @param {string} [keyData]
   */
  range(start, stop, keyRank = this.keyRank(), keyData = this.keyData()) {
    throw new Error('RTLeaderboard is virtual func. You must override this!');
  }

  /**
   * @description 取得整个榜单数据
   * @param {string} [keyRank]
   * @param {string} [keyData]
   * @return {Promise<void>}
   */
  async all(keyRank = this.keyRank(), keyData = this.keyData()) {
    const list = await this.range(0, this.maxNum, keyRank, keyData);
    Log.debug(`${this.name} all list:${JSON.stringify(list)}`);
    return list;
  }

  /**
   * @description 保存整个榜单数据
   * @param {string} [keyRank]
   * @param {string} [keyData]
   * @param {string} [keyAll]
   * @return {Promise<*>}
   */
  async save(keyRank = this.keyRank(), keyData = this.keyData(), keyAll = this.keyAll()) {
    let allData = await this.all(keyRank, keyData);
    return this._conn.set(keyAll, JSON.stringify(allData));
  }

  /**
   * @description 取得保存数据
   * @param {string} [keyAll]
   * @return {Promise<*>}
   */
  async load(keyAll = this.keyAll()) {
    let result = await this._conn.get(keyAll);
    return JSON.parse(result);
  }

  /**
   * @description 清理榜单所有数据
   * @param {string} [keyRank]
   * @param {string} [keyData]
   * @return {Promise<void>}
   */
  async clean(keyRank = this.keyRank(), keyData = this.keyData()) {
    this._conn.del(keyRank);
    this._conn.del(keyData);
  }

  release() {
    delete this._conn;
    delete this._maxNum;
    delete this._keyRank;
    delete this._keyData;
  }
}

module.exports = RTLeaderboard;