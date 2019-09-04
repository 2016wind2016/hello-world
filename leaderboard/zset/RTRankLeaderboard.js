/**
 * 实时财富排行榜
 * Created by wangkm on 2019-8-30.
 * Email: 417079820@qq.com.
 */
const WW = require('../../../core');
const Log = require('../../../../lib/log');
const RTScoreLeaderboard = require('./RTScoreLeaderboard');

class RTRankLeaderboard extends RTScoreLeaderboard {
  constructor({conn, maxNum, keyRank, keyData}) {
    super({conn, maxNum, keyRank, keyData});
  }

  /**
   * @description 取得赛季ID,每秒才能取一次，避免太频繁
   * @return {number}
   */
  get seasonId() {
    if (!this._seasonId) {
      this._seasonId = WW.PVP.getCurSeasonId();
      clearTimeout(this._seasonIdTimer);
      this._seasonIdTimer = setTimeout(() => {
        this._seasonId = null;
      }, 1500);
    }
    return this._seasonId;
  }

  keyRank(keyRank = this._keyRank, seasonId = this.seasonId) {
    return `${this.name}:${keyRank}:${seasonId}`;
  }

  keyData(keyData = this._keyData, seasonId = this.seasonId) {
    return `${this.name}:${keyData}:${seasonId}`;
  }

  keyAll(keyAll = this._keyData, seasonId = this.seasonId) {
    return `${this.name}:${keyAll}:${seasonId}`;
  }

  /**
   * !排行榜赛季切换请主动调用此逻辑保存榜单
   * @description 保存整个榜单数据
   * @param {string} [keyRank]
   * @param {string} [keyData]
   * @param {string} [keyAll]
   * @param {string} [seasonId]
   * @return {Promise<*>}
   */
  async save(keyRank = this.keyRank(), keyData = this.keyData(), keyAll = this.keyAll(), seasonId = this.seasonId()) {
    let allData = await this.all(keyRank, keyData);
    Log.debug(`${this.name} save ${JSON.stringify(allData)}`);
    return this._conn.hset(keyAll, seasonId, JSON.stringify(allData));
  }

  /**
   * !旧赛季排行榜数据会清除、读取请使用此接口
   * @description 取得保存数据
   * @param {string} [keyAll]
   * @param {string} [seasonId]
   * @return {Promise<*>}
   */
  async load(keyAll = this.keyAll(), seasonId = this.seasonId()) {
    let result = await this._conn.hget(keyAll, seasonId);
    Log.debug(`${this.name} load ${JSON.parse(result)}`);
    return JSON.parse(result);
  }

  /**
   * !赛季切换及时清理榜单数据、减少存储空间
   * @description 清理榜单所有数据
   * @param {string} [keyRank]
   * @param {string} [keyData]
   * @return {Promise<void>}
   */
  clean(keyRank = this.keyRank(), keyData = this.keyData()) {
    return super.clean(keyRank, keyData);
  }
}

module.exports = RTRankLeaderboard;