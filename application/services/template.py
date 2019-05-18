import json
import datetime
import uuid
from logging import getLogger
from nameko.rpc import rpc, RpcProxy
import bson.json_util

_log = getLogger(__name__)


class DateEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (datetime.datetime, datetime.date)):
            return o.isoformat()
        return json.JSONEncoder.default(self, o)


class TemplateServiceError(Exception):
    pass


class TemplateService(object):
    name = 'template'

    metadata = RpcProxy('metadata')
    datareader = RpcProxy('datareader')
    referential = RpcProxy('referential')
    svg_builder = RpcProxy('svg_builder')
    subscription = RpcProxy('subscription_manager')
    exporter = RpcProxy('exporter')

    @staticmethod
    def _get_display_name(entity, language):
        if 'internationalization' in entity and language in entity['internationalization']:
            return entity['internationalization'][language]
        return entity['common_name']

    @staticmethod
    def _get_short_name(entity, language):
        if 'informations' in entity and entity['informations']\
        and 'last_name' in entity['informations'] and 'known' in entity['informations']:
            if entity['informations']['known']:
                return entity['informations']['known']
            return entity['informations']['last_name']

        return TemplateService._get_display_name(entity, language)

    @staticmethod
    def _get_multiline_name(entity, language):
        if 'multiline' in entity and entity['multiline']:
            return entity['multiline']
        if 'informations' in entity and entity['informations']\
        and 'last_name' in entity['informations'] and 'first_name' in entity['informations']:
            return {'first_name': entity['informations']['first_name'], 'last_name': entity['informations']['last_name']}
        return {'first_name': '', 'last_name': TemplateService._get_display_name(entity, language)}

    def _append_picture_into_referential_results(self, entry_key, referential_results, json_only, context, _format, user):
        entry_id = referential_results[entry_key]['id']
        if 'picture' not in referential_results[entry_key]:
            referential_results[entry_key]['picture'] = {}

        if _format not in referential_results[entry_key]['picture']:
            referential_results[entry_key]['picture'][_format] = None

        if json_only is False:
            _log.info('Appending picture to referential entry {} (context: {} / format: {})'\
                .format(entry_id, context, _format))
            picture = self.referential.get_entity_picture(entry_id, context, _format, user)
            if not picture:
                msg = 'Picture not found for referential entry: {} (context: {} / format: {})'.format(entry_id, context, _format)
                _log.error(msg)
                raise TemplateServiceError(msg)
            referential_results[entry_key]['picture'][_format] = picture

    def _handle_referential(self, referential, language, json_only, user):
        _log.info('Gathering referential entries ...')
        results = dict()
        for k,v in referential.items():
            if 'id' not in v or 'event_or_entity' not in v:
                raise TemplateServiceError('Wrong formated referential entry (id and event_or_entity are mandatory)')
            _log.info('Trying to retrieve referential entry {} which has been set under key {}'.format(v['id'], k))
            current_ref_str = None
            if v['event_or_entity'] == 'entity':
                current_ref_str = self.referential.get_entity_by_id(v['id'], user)
            else:
                current_ref_str = self.referential.get_event_by_id(v['id'], user)
            if not current_ref_str:
                raise TemplateServiceError('Referential entry not found: {}'.format(v['id']))
            results[k] = bson.json_util.loads(current_ref_str)
            results[k]['display_name'] = self._get_display_name(results[k], language)
            results[k]['short_name'] = self._get_short_name(results[k], language)
            results[k]['multiline_name'] = self._get_multiline_name(results[k], language)
        return results

    def _get_query_parameters_and_append_pictures(self, q, current_query, user_parameters, referential_results, json_only, context, user):
        current_id = q['id']
        parameters = list()
        if current_query['parameters']:
            for p in current_query['parameters']:
                if user_parameters is not None:
                    if current_id in user_parameters and p in user_parameters[current_id]:
                        parameters.append(user_parameters[current_id][p])
                if 'referential_parameters' in q and q['referential_parameters']:
                    for ref in q['referential_parameters']:
                        if p in ref:
                            entry_id = referential_results[ref[p]['name']]['id']
                            parameters.append(entry_id)
                            if 'picture' in ref[p] and json_only is False:
                                if 'format' not in ref[p]['picture']:
                                    raise TemplateServiceError('Format not in picture configuration for referential parameter {}'.format(p))
                                _format = ref[p]['picture']['format']
                                self._append_picture_into_referential_results(ref[p]['name'], referential_results, json_only, context, _format, user)
        _log.info("Following parameters:{} has been built and will be applied to the query {}".format(parameters, current_id))
        return parameters

    def _labelize_row(self, row, q, language, context, user):
        _log.info('Translating labels into {} ...'.format(language)) 
        labelized_row = row.copy()
        if 'labels' in q and q['labels']:
            current_labels = q['labels']
            for lab in current_labels:
                if lab in row:
                    if current_labels[lab] == 'entity':
                        current_entity = bson.json_util.loads(self.referential.get_entity_by_id(row[lab], user))
                        labelized_row[lab] = current_entity['common_name']
                    elif current_labels[lab] == 'label':
                        current_label = self.referential.get_labels_by_id_and_language_and_context(row[lab], language, context)
                        if current_label is None:
                            raise TemplateServiceError('Label {} not found'.format(row[lab]))
                        labelized_row[lab] = current_label['label']
        return labelized_row

    def _append_referential_results(self, row, q, referential_results, json_only, context, language, user):
        _log.info('Appending all referential results ...')
        current_ref_config = q['referential_results']
        for cfg in current_ref_config:
            if current_ref_config[cfg]['event_or_entity'] == 'event':
                current_ref_result = bson.json_util.loads(self.referential.get_event_by_id(row[cfg], user))
                if not current_ref_result:
                    raise TemplateServiceError('Event {} not found'.format(row[cfg]))
            else:
                current_ref_result = bson.json_util.loads(self.referential.get_entity_by_id(row[cfg], user))
                if not current_ref_result:
                    raise TemplateServiceError('Entity {} not found'.format(row[cfg]))
                current_ref_result['display_name'] = self._get_display_name(current_ref_result, language)
                current_ref_result['short_name'] = self._get_short_name(current_ref_result, language)
                current_ref_result['multiline_name'] = self._get_multiline_name(current_ref_result, language)
            current_column_id = current_ref_config[cfg]['column_id']
            referential_results[row[current_column_id]] = current_ref_result
            if 'picture' in current_ref_config[cfg] and json_only is False:
                self._append_picture_into_referential_results(row[current_column_id], referential_results, json_only, context,
                    current_ref_config[cfg]['picture']['format'], user)

    def _get_template_data(self, template, picture_context, language, json_only, referential, user_parameters, user):
        _log.info('Building template data ...')
        context = template['context']
        referential_results = dict()
        if referential is not None:
            referential_results = self._handle_referential(referential, language, json_only, user)

        query_results = dict()
        for q in template['queries']:
            query_results[q['id']] = dict()
            current_query = bson.json_util.loads(self.metadata.get_query(q['id']))
            current_sql = current_query['sql']
            current_id = q['id']
            current_limit = int(q['limit']) if 'limit' in q and isinstance(q['limit'], int) else 50
            _log.info('Query will be limited to {} rows (a negative value means no limit)'.format(str(current_limit)))
            parameters = self._get_query_parameters_and_append_pictures(q, current_query, user_parameters, referential_results, json_only, picture_context, user)
            try:
                current_results = bson.json_util.loads(self.datareader.select(current_sql, parameters, limit=current_limit))
            except:
                raise TemplateServiceError('An error occured while executing query {}'.format(current_id))
            if not current_results:
                raise TemplateServiceError('Query {} returns nothing'.format(current_id))
            labelized_results = list()
            for row in current_results:
                labelized_results.append(self._labelize_row(row, q, language, context, user))
                if 'referential_results' in q and q['referential_results']:
                    self._append_referential_results(row, q, referential_results, json_only, picture_context, language, user)
            query_results[q['id']] = labelized_results
        results = {'referential': referential_results, 'query': query_results}
        return results

    @staticmethod
    def _pick_picture_context(template, picture_context):
        _log.info('Picking the right picture context')
        if picture_context:
            return picture_context
        
        if 'picture' in template and template['picture'] and 'context' in template['picture']:
            return template['picture']['context']

        _log.info('No picture context have been picked ...')
        return None

    @rpc
    def resolve(self, template_id, picture_context, language, json_only, referential, user_parameters,
    user, text_to_path):
        _log.info('{} is resolving template {} ...'.format(user, template_id))
        _log.info('Picture context: {}'.format(picture_context))
        _log.info('Language: {}'.format(language))
        template = bson.json_util.loads(self.metadata.get_template(template_id, user))
        template_language = language if language else template['language']
        _log.info('Template will be resolved in {}'.format(template_language))
        tmpl_pic_ctx = self._pick_picture_context(template, picture_context)

        results = self._get_template_data(template, tmpl_pic_ctx, template_language, json_only,
        referential, user_parameters, user)
        json_results = json.dumps(results, cls=DateEncoder)

        if json_only is True:
            return {'content': json_results, 'mimetype': 'application/json'}

        if template['kind'] == 'image':
            try:
                _log.info('Merging data and SVG template ...')
                infography = self.svg_builder.replace_jsonpath(template['svg'], json.loads(json_results))
            except:
                raise TemplateServiceError('Wrong formated template !')

            if text_to_path is True:
                _log.info('Converting text into path in generated SVG ...')
                return {'content': self.exporter.text_to_path(infography), 'mimetype': 'image/svg+xml'}

            return {'content': infography, 'mimetype': 'image/svg+xml'}
        else:
            sub = bson.json_util.loads(self.subscription.get_subscription_by_user(user))
            if 'export' not in sub['subscription']:
                raise TemplateServiceError('Export not configured for user {}'.format(user))
            export_config = sub['subscription']['export']
            filename = template['datasource'] if 'datasource' in template and template['datasource'] else "{}.json".format(str(uuid.uuid4()))
            _log.info('Uploading JSON data on user\'s configured datasource ...')
            url = self.exporter.upload(json_results, filename, export_config)
            html = template['html']
            if '${DATASOURCE}' not in template['html']:
                raise TemplateServiceError('Missing DATASOURCE variable in HTML template')
            return {'content': html.replace('${DATASOURCE}', url), 'mimetype':'text/html'}
