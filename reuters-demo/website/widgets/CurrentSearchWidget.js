(function ($) {

AjaxSolr.CurrentSearchWidget = AjaxSolr.AbstractWidget.extend({
  afterRequest: function () {
    var self = this;
    var links = [];

    var fq = this.manager.store.values('fq');
    for (var i = 0, l = fq.length; i < l; i++) {
      links.push($('<a href="#"/>').text('(x) ' + fq[i]).click(self.removeFacet(fq[i])));
    }

    if (links.length > 1) {
      links.unshift($('<a href="#"/>').text('remove all').click(function () {
        self.manager.store.remove('fq');
        self.manager.doRequest(0);
        return false;
      }));
    }

    if (links.length) {
      AjaxSolr.theme('list_items', this.target, links);
    }
    else {
      $(this.target).html('<div>Viewing all documents!</div>');
    }
  },

  removeFacet: function (facet) {
    var self = this;
    return function () {
      if (self.manager.store.removeByValue('fq', facet)) {
        self.manager.doRequest(0);
      }
      return false;
    };
  }
});

})(jQuery);
