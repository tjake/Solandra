// $Id$

/**
 * Offers an interface to the local parameters used by the Spatial Solr plugin.
 *
 * @see http://www.jteam.nl/news/spatialsolr
 *
 * @class AbstractSpatialWidget
 * @augments AjaxSolr.AbstractWidget
 */
AjaxSolr.AbstractSpatialWidget = AjaxSolr.AbstractWidget.extend(
  /** @lends AjaxSolr.AbstractSpatialWidget.prototype */
  {
  /**
   * Sets the Spatial Solr local parameters.
   *
   * @param {Object} params The local parameters to set.
   * @param {Number} params.lat Latitude of the center of the search area.
   * @param {Number} params.lng Longitude of the center of the search area.
   * @param {Number} params.radius Radius of the search area.
   * @param {String} [params.unit] Unit the distances should be calculated in:
   *   "km" or "miles".
   * @param {String} [params.calc] <tt>GeoDistanceCalculator</tt> that will be
   *   used to calculate the distances. "arc" for
   *   <tt>ArchGeoDistanceCalculator</tt> and "plane" for
   *   <tt>PlaneGeoDistanceCalculator</tt>.
   * @param {Number} [params.threadCount] Number of threads that will be used
   *   by the <tt>ThreadedDistanceFilter</tt>.
   */
  set: function (params) {
    this.manager.store.get('q').local('type', 'spatial');
    this.manager.store.get('q').local('lat', params.lat);
    this.manager.store.get('q').local('long', params.lng);
    this.manager.store.get('q').local('radius', params.radius);
    if (params.unit !== undefined) {
      this.manager.store.get('q').local('unit', params.unit);
    }
    if (params.calc !== undefined) {
      this.manager.store.get('q').local('calc', params.calc);
    }
    if (params.threadCount !== undefined) {
      this.manager.store.get('q').local('threadCount', params.threadCount);
    }
  },

  /**
   * Removes the Spatial Solr local parameters.
   */
  clear: function () {
    this.manager.store.get('q').remove('type');
    this.manager.store.get('q').remove('lat');
    this.manager.store.get('q').remove('long');
    this.manager.store.get('q').remove('radius');
    this.manager.store.get('q').remove('unit');
    this.manager.store.get('q').remove('calc');
    this.manager.store.get('q').remove('threadCount');
  }
});
