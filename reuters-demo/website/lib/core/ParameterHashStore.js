// $Id$

/**
 * A parameter store that stores the values of exposed parameters in the URL
 * hash to maintain the application's state.
 *
 * <p>The ParameterHashStore observes the hash for changes and loads Solr
 * parameters from the hash if it observes a change or if the hash is empty.</p>
 *
 * @class ParameterHashStore
 * @augments AjaxSolr.ParameterStore
 */
AjaxSolr.ParameterHashStore = AjaxSolr.ParameterStore.extend(
  /** @lends AjaxSolr.ParameterHashStore.prototype */
  {
  /**
   * The interval in milliseconds to use in <tt>setInterval()</tt>. Do not set
   * the interval too low as you may set up a race condition. 
   *
   * @field
   * @public
   * @type Number
   * @default 250
   * @see ParameterHashStore#init()
   */
  interval: 250,

  /**
   * Reference to the setInterval() function.
   *
   * @field
   * @private
   * @type Function
   */
  intervalId: null,

  /**
   * A local copy of the URL hash, so we can detect changes to it.
   *
   * @field
   * @private
   * @type String
   * @default ""
   */
  hash: '',

  /**
   * If loading and saving the hash take longer than <tt>interval</tt>, we'll
   * hit a race condition. However, this should never happen.
   */
  init: function () {
    if (this.exposed.length) {
      this.intervalId = window.setInterval(this.intervalFunction(this), this.interval);
    }
  },

  /**
   * Stores the values of the exposed parameters in both the local hash and the
   * URL hash. No other code should be made to change these two values.
   */
  save: function () {
    this.hash = this.exposedString();
    if (this.storedString()) {
      // make a new history entry
      window.location.hash = this.hash;
    }
    else {
      // replace the old history entry
      window.location.replace(window.location.href.replace('#', '') + '#' + this.hash);
    }
  },

  /**
   * @see ParameterHash#storedString()
   */
  storedString: function () {
    // Some browsers automatically unescape characters in the hash, others
    // don't. Fortunately, all leave window.location.href alone. So, use that.
    var index = window.location.href.indexOf('#');
    if (index == -1) {
      return '';
    }
    else {
      return window.location.href.substr(index + 1);
    }
  },

  /**
   * Checks the hash for changes, and loads Solr parameters from the hash and
   * sends a request to Solr if it observes a change or if the hash is empty
   */
  intervalFunction: function (self) {
    return function () {
      // Support the back/forward buttons. If the hash changes, do a request.
      var hash = self.storedString();
      if (hash.length) {
        if (self.hash != hash) {
          self.load();
          self.manager.doRequest();
        }
      }
      else {
        // AJAX Solr is loading for the first time, or the user deleted the hash.
        self.manager.doRequest();
      }
    }
  }
});