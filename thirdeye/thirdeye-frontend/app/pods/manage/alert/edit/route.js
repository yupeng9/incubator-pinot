/**
 * Handles the 'edit' route for manage alert
 * @module manage/alert/edit/edit
 * @exports manage/alert/edit/edit
 */
import fetch from 'fetch';
import moment from 'moment';
import _ from 'lodash';
import Route from '@ember/routing/route';
import { checkStatus, buildDateEod, parseProps } from 'thirdeye-frontend/helpers/utils';

 /**
 * Builds the graph metric URL from config settings
 * @param {Object} cfg - settings for current metric graph
 * @param {String} maxTime - an 'bookend' for this metric's graphable data
 * @return {String} URL for graph metric API
 */
const buildGraphConfig = (config, maxTime) => {
  const dimension = config.exploreDimensions ? config.exploreDimensions.split(',')[0] : 'All';
  const currentEnd = moment(maxTime).isValid()
    ? moment(maxTime).valueOf()
    : buildDateEod(1, 'day').valueOf();
  const formattedFilters = JSON.stringify(parseProps(config.filters));
  // Load less data if granularity is 'minutes'
  const isMinutely = config.bucketUnit.toLowerCase().includes('minute');
  const duration = isMinutely ? { unit: 2, size: 'week' } : { unit: 1, size: 'month' };
  const currentStart = moment(currentEnd).subtract(duration.unit, duration.size).valueOf();
  const baselineStart = moment(currentStart).subtract(1, 'week').valueOf();
  const baselineEnd = moment(currentEnd).subtract(1, 'week');

  // Prepare call for metric graph data
  const metricDataUrl =  `/timeseries/compare/${config.id}/${currentStart}/${currentEnd}/` +
    `${baselineStart}/${baselineEnd}?dimension=${dimension}&granularity=` +
    `${config.bucketSize + '_' + config.bucketUnit}&filters=${encodeURIComponent(formattedFilters)}`;

  // Prepare call for dimension graph data
  const topDimensionsUrl = `/rootcause/query?framework=relatedDimensions&anomalyStart=${currentStart}` +
    `&anomalyEnd=${currentEnd}&baselineStart=${baselineStart}&baselineEnd=${baselineEnd}` +
    `&analysisStart=${currentStart}&analysisEnd=${currentEnd}&urns=thirdeye:metric:${config.id}` +
    `&filters=${encodeURIComponent(formattedFilters)}`;

  return { metricDataUrl, topDimensionsUrl };
};

export default Route.extend({
  model(params) {
   const { id, alertData, email, allConfigGroups, allAppNames } = this.modelFor('manage.alert');
    if (!id) { return; }

    return {
       alertData,
       email,
       allConfigGroups,
       allAppNames
    };
  },

  afterModel(model) {
   const {
      alertData,
      email: groupByAlertId,
      allConfigGroups,
      allAppNames
   } = model;

   const {
      id,
      metric: metricName,
      collection: dataset,
      exploreDimensions,
      filters,
      bucketSize,
      bucketUnit,
      properties: alertProps
    } = alertData;

    let metricId = '';
    let metricDataUrl = '';
    let metricDimensionURl = '';

    // Add a parsed properties array to the model
    const propsArray = alertProps.split(';').map((prop) => {
      const [ name, value ] = prop.split('=');
      return { name, value: decodeURIComponent(value) };
    });

    const originalConfigGroup = groupByAlertId.length ? groupByAlertId.pop() : null;
    const selectedAppName = originalConfigGroup ? originalConfigGroup.application : null;
    const selectedApplication = _.find(allAppNames, function(appsObj) { return appsObj.application === selectedAppName; });

    Object.assign(model, {
      propsArray,
      allConfigGroups: _.uniq(allConfigGroups, name),
      originalConfigGroup,
      selectedAppName,
      allApps: allAppNames,
      selectedApplication,
      exploreDimensions
    });

    // Package settings for metric and dimension data calls needed for graph rendering
    const metricSettings = {
      filters,
      bucketSize,
      bucketUnit,
      exploreDimensions
    };

    return fetch(`/data/autocomplete/metric?name=${dataset}::${metricName}`).then(checkStatus)
      .then((metricsByName) => {
        const metric = metricsByName.pop();
        metricId = metric.id;
        metricSettings.id = metricId;
        return fetch(`/data/maxDataTime/metricId/${metricId}`).then(checkStatus);
      })
      .then((maxTime) => {
        const { metricDataUrl, topDimensionsUrl } = buildGraphConfig(metricSettings, maxTime);
        Object.assign(model, { metricDataUrl, topDimensionsUrl });
      })
      // Got errors?
      .catch((err) => {
        console.log('err : ', err);
        Object.assign(model, { loadError: true, loadErrorMsg: err });
      });
  },

  resetController(controller, isExiting) {
    this._super(...arguments);

    if (isExiting) {
      controller.clearAll();
    }
  },

  setupController(controller, model) {
    this._super(controller, model);

    controller.setProperties({
      model,
      metricName: model.alertData.metric,
      granularity: model.alertData.bucketSize + '_' + model.alertData.bucketUnit,
      alertFilters: model.alertData.filters,
      alertProps: model.propsArray,
      alertConfigGroups: model.allConfigGroups,
      alertFunctionName: model.alertData.functionName,
      alertId: model.alertData.id,
      isActive: model.alertData.isActive,
      allApplications: model.allApps,
      selectedConfigGroup: model.originalConfigGroup,
      selectedApplication: model.selectedApplication,
      selectedAppName: model.selectedAppName,
      isLoadError: model.loadError,
      loadErrorMessage: model.loadErrorMsg
    });
  },

  actions: {
    /**
     * Action called on submission to reload the route's model
     */
    refreshModel() {
      this.refresh();
    },

    didTransition() {
      const {
        metricDataUrl,
        topDimensionsUrl,
        exploreDimensions
      } = this.currentModel;

      fetch(metricDataUrl).then(checkStatus)
        .then((metricData) => {
          const isMetricDataLoading = exploreDimensions ? true : false;
          Object.assign(metricData, { color: 'blue' });
          this.controller.setProperties({ metricData, isMetricDataLoading });
          if (exploreDimensions) {
            return fetch(topDimensionsUrl).then(checkStatus)
              .then((metricDimensions) => {
                this.controller.setProperties({
                  metricDimensions,
                  isMetricDataLoading: false,
                  alertDimension: exploreDimensions.split(',')[0]
                });
              });
          }
        })
        .catch((errors) => {
          this.controller.setProperties({
            isMetricDataInvalid: true,
            isMetricDataLoading: false,
            graphMessageText: 'Error loading metric data'
          });
        });
    }
  }
});
