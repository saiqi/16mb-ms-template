import json
import datetime
import os
import uuid
from logging import getLogger, basicConfig
from nameko.rpc import rpc, RpcProxy
from nameko.events import event_handler, BROADCAST
from nameko.dependency_providers import DependencyProvider
import bson.json_util

_log = getLogger(__name__)

CDN_ROOT_URL = os.getenv('CDN_ROOT_URL')

class ErrorHandler(DependencyProvider):

    def worker_result(self, worker_ctx, res, exc_info):
        if exc_info is None:
            return
        exc_type, exc, tb = exc_info
        _log.error(str(exc))


class DateEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (datetime.datetime, datetime.date)):
            return o.isoformat()
        return json.JSONEncoder.default(self, o)


class TemplateServiceError(Exception):
    pass


class TemplateService(object):
    name = 'template'
    error = ErrorHandler()
    metadata = RpcProxy('metadata')
    datareader = RpcProxy('datareader')
    referential = RpcProxy('referential')
    svg_builder = RpcProxy('svg_builder')
    subscription = RpcProxy('subscription_manager')
    exporter = RpcProxy('exporter')
    notifier = RpcProxy('notifier')

    @staticmethod
    def _get_overriden_name(entity, language):
        return entity.get('internationalization', {}).get(language)

    @staticmethod
    def _get_display_name(entity, language):
        if TemplateService._get_overriden_name(entity, language):
            return TemplateService._get_overriden_name(entity, language)

        return entity['common_name']

    @staticmethod
    def _get_short_name(entity, language):
        if TemplateService._get_overriden_name(entity, language):
            return TemplateService._get_overriden_name(entity, language)

        if 'informations' in entity and entity['informations']\
                and 'last_name' in entity['informations'] and 'known' in entity['informations']:
            if entity['informations']['known']:
                return entity['informations']['known']
            return entity['informations']['last_name']

        return entity['common_name']

    @staticmethod
    def _get_multiline_name(entity, language):
        if 'multiline' in entity and entity['multiline']:
            return entity['multiline']
        if 'informations' in entity and entity['informations']\
                and 'last_name' in entity['informations'] and 'first_name' in entity['informations']:
            return {'first_name': entity['informations']['first_name'], 'last_name': entity['informations']['last_name']}
        return {'first_name': '', 'last_name': TemplateService._get_display_name(entity, language)}

    def _append_picture_into_referential_results(self, entry_key, referential_results, json_only, context, _format, kind, user):
        entry_id = referential_results[entry_key]['id']
        if 'picture' not in referential_results[entry_key]:
            referential_results[entry_key]['picture'] = {}

        if _format not in referential_results[entry_key]['picture']:
            referential_results[entry_key]['picture'][_format] = None

        if json_only is False:
            picture = self.referential.get_entity_picture(
                entry_id, context, _format, user, kind)
            if not picture:
                raise TemplateServiceError('Picture not found for referential entry: {} (context: {} / format: {})'.format(
                    entry_id, context, _format))
            referential_results[entry_key]['picture'][_format] = picture

    def _handle_referential(self, referential, language, json_only, user):
        _log.info('Gathering referential entries ...')
        results = dict()
        for k, v in referential.items():
            if 'id' not in v or 'event_or_entity' not in v:
                raise TemplateServiceError(
                    'Wrong formated referential entry (id and event_or_entity are mandatory)')
            _log.info(
                'Trying to retrieve referential entry {} which has been set under key {}'.format(v['id'], k))
            current_ref_str = None
            if v['event_or_entity'] == 'entity':
                current_ref_str = self.referential.get_entity_by_id(
                    v['id'], user)
            else:
                current_ref_str = self.referential.get_event_by_id(
                    v['id'], user)
            if not current_ref_str:
                raise TemplateServiceError(
                    'Referential entry not found: {}'.format(v['id']))
            results[k] = bson.json_util.loads(current_ref_str)
            results[k]['display_name'] = self._get_display_name(
                results[k], language)
            results[k]['short_name'] = self._get_short_name(
                results[k], language)
            results[k]['multiline_name'] = self._get_multiline_name(
                results[k], language)
        return results

    def _get_query_parameters_and_append_pictures(self, q, current_query, user_parameters, referential_results, json_only, context, user):
        current_id = q['id']
        parameters = list()
        if not current_query['parameters']:
            return None
        for p in current_query['parameters']:
            if user_parameters and current_id in user_parameters and p in user_parameters[current_id]:
                parameters.append(user_parameters[current_id][p])
            if 'referential_parameters' not in q or not q['referential_parameters']:
                continue
            for ref in filter(lambda x: p in x, q['referential_parameters']):
                entry_id = referential_results[ref[p]['name']]['id']
                parameters.append(entry_id)
                if 'picture' in ref[p] and json_only is False:
                    if 'format' not in ref[p]['picture']:
                        raise TemplateServiceError(
                            'Format not in picture configuration for referential parameter {}'.format(p))
                    _format = ref[p]['picture']['format']
                    kind = ref[p]['picture'].get('kind', 'bitmap')
                    self._append_picture_into_referential_results(
                        ref[p]['name'], referential_results, json_only, context, _format, kind, user)
        _log.info("Following parameters:{} has been built and will be applied to the query {}".format(
            parameters, current_id))
        return parameters

    def _labelize_row(self, row, q, language, context, user):
        labelized_row = row.copy()
        if 'labels' not in q or not q['labels']:
            return labelized_row
        current_labels = q['labels']
        for lab in current_labels:
            if lab not in row:
                continue
            if current_labels[lab] == 'entity':
                current_entity = bson.json_util.loads(
                    self.referential.get_entity_by_id(row[lab], user))
                labelized_row[lab] = current_entity['common_name']
            elif current_labels[lab] == 'label':
                current_label = self.referential.get_labels_by_id_and_language_and_context(
                    row[lab], language, context)
                if current_label is None:
                    raise TemplateServiceError(
                        'Label {} not found'.format(row[lab]))
                labelized_row[lab] = current_label['label']
        return labelized_row

    def _append_referential_results(self, row, q, referential_results, json_only, context, language, user):
        current_ref_config = q['referential_results']
        for cfg in current_ref_config:
            if current_ref_config[cfg]['event_or_entity'] == 'event':
                current_ref_result = bson.json_util.loads(
                    self.referential.get_event_by_id(row[cfg], user))
                if not current_ref_result:
                    raise TemplateServiceError(
                        'Event {} not found'.format(row[cfg]))
            else:
                current_ref_result = bson.json_util.loads(
                    self.referential.get_entity_by_id(row[cfg], user))
                if not current_ref_result:
                    raise TemplateServiceError(
                        'Entity {} not found'.format(row[cfg]))
                current_ref_result['display_name'] = self._get_display_name(
                    current_ref_result, language)
                current_ref_result['short_name'] = self._get_short_name(
                    current_ref_result, language)
                current_ref_result['multiline_name'] = self._get_multiline_name(
                    current_ref_result, language)
            current_column_id = current_ref_config[cfg]['column_id']
            referential_results[row[current_column_id]] = current_ref_result
            if 'picture' in current_ref_config[cfg] and json_only is False:
                self._append_picture_into_referential_results(row[current_column_id], referential_results, json_only, context,
                                                              current_ref_config[cfg]['picture']['format'],
                                                              current_ref_config[cfg]['picture'].get('kind', 'bitmap'),
                                                              user)

    def _get_template_data(self, template, picture_context, language, json_only, referential, user_parameters, user):
        _log.info('Building template data ...')
        context = template['context']
        referential_results = dict()
        if referential is not None:
            referential_results = self._handle_referential(
                referential, language, json_only, user)

        query_results = dict()
        for q in template['queries']:
            query_results[q['id']] = dict()
            current_query = bson.json_util.loads(
                self.metadata.get_query(q['id']))
            current_sql = current_query['sql']
            current_id = q['id']
            current_limit = int(q['limit']) if 'limit' in q and isinstance(
                q['limit'], int) else 50
            _log.info('Query will be limited to {} rows (a negative value means no limit)'.format(
                str(current_limit)))
            parameters = self._get_query_parameters_and_append_pictures(
                q, current_query, user_parameters, referential_results, json_only, picture_context, user)
            try:
                current_results = bson.json_util.loads(self.datareader.select(
                    current_sql, parameters, limit=current_limit))
            except:
                raise TemplateServiceError(
                    'An error occured while executing query {}'.format(current_id))
            if not current_results:
                raise TemplateServiceError(
                    'Query {} returns nothing'.format(current_id))
            labelized_results = list()
            for row in current_results:
                labelized_results.append(self._labelize_row(
                    row, q, language, context, user))
                if 'referential_results' in q and q['referential_results']:
                    self._append_referential_results(
                        row, q, referential_results, json_only, picture_context, language, user)
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

    @staticmethod
    def _handle_trigger_referential_params(referential_params, event_id):
        event = {'id': event_id, 'event_or_entity': 'event'}
        return dict((k, v if 'from_event' not in v else event) for k, v in referential_params.items())

    @rpc
    def resolve(self, template_id, picture_context, language, json_only, referential, user_parameters,
                user, text_to_path):
        _log.info('{} is resolving template {} ...'.format(user, template_id))
        _log.info('Picture context: {}'.format(picture_context))
        _log.info('Language: {}'.format(language))
        template = bson.json_util.loads(
            self.metadata.get_template(template_id, user))
        if not template:
            raise TemplateServiceError(
                f'Template {template_id} not found or {user} not allowed to resolve template !')
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
                infography = self.svg_builder.replace_jsonpath(
                    template['svg'], json.loads(json_results))
            except:
                raise TemplateServiceError('Wrong formated template !')

            if text_to_path is True:
                _log.info('Converting text into path in generated SVG ...')
                return {'content': self.exporter.text_to_path(infography), 'mimetype': 'image/svg+xml'}

            return {'content': self.exporter.to_plain_svg(infography), 'mimetype': 'image/svg+xml'}
        else:
            sub = bson.json_util.loads(
                self.subscription.get_subscription_by_user(user))
            if 'export' not in sub['subscription']:
                raise TemplateServiceError(
                    'Export not configured for user {}'.format(user))
            export_config = sub['subscription']['export']
            filename = template['datasource'] if 'datasource' in template and template['datasource'] else "{}.json".format(
                str(uuid.uuid4()))
            _log.info('Uploading JSON data on user\'s configured datasource ...')
            url = self.exporter.upload(json_results, filename, export_config)
            html = template['html']

            if '${DATASOURCE}' not in template['html']:
                raise TemplateServiceError(
                    'Missing DATASOURCE variable in HTML template!')
            return {
                'content': html.replace('${DATASOURCE}', url).replace('${CDN_ROOT_URL}', CDN_ROOT_URL or ''),
                'mimetype': 'text/html'
            }

    @event_handler(
        'loader', 'input_loaded', handler_type=BROADCAST, reliable_delivery=False)
    def handle_input_loaded(self, payload):
        msg = bson.json_util.loads(payload)
        if 'meta' not in msg or 'id' not in msg:
            _log.warning('Inoperable input received !')
            return
        meta = msg['meta']
        if 'source' not in meta or 'type' not in meta:
            _log.warning('Inoperable meta in received input !')
            return
        _log.info(
            f'Input event {msg["id"]} received, checking if there is a trigger to refresh ...')
        on_event = {'source': meta['source'], 'type': meta['type']}
        #####
        triggers = bson.json_util.loads(
            self.metadata.get_fired_triggers(on_event))
        content_id = meta.get('content_id', msg['id'])
        for t in triggers:
            sub = bson.json_util.loads(
                self.subscription.get_subscription_by_user(t['user']))
            if 'export' not in sub['subscription']:
                _log.warning(f'Export not configured for user {t["user"]}')
                continue
            export_config = sub['subscription']['export']
            res = self.referential.get_event_filtered_by_entities(content_id,
                                                                  t['selector'], t['user'])
            event = bson.json_util.loads(res)
            if not event:
                _log.info('No event has been found !')
                continue

            _log.info(f'Refreshing trigger {t["id"]} on event {event["id"]}')
            spec = t['template']
            template = bson.json_util.loads(
                self.metadata.get_template(spec['id'], t['user']))
            if not template:
                _log.error(f'Template {spec["id"]} not found')
                continue
            picture_context = None
            if template['picture']:
                picture_context = template['picture']['context']
            if 'picture' in spec and 'context' in spec['picture']:
                picture_context = spec['picture']['context']

            language = spec.get('language', template['language'])
            json_only = spec.get('json_only', False)
            referential = None
            if 'referential' in spec:
                referential = self._handle_trigger_referential_params(
                    spec['referential'], content_id)
            user_parameters = spec.get('user_parameters', None)

            result = self._get_template_data(template, picture_context, language, json_only,
                                             referential, user_parameters, t['user'])
            json_results = json.dumps(result, cls=DateEncoder)
            if json_only and t['export']['format'] == 'json':
                url = self.exporter.upload(
                    json_results, t['export']['filename'], export_config)
            else:
                infography = self.svg_builder.replace_jsonpath(
                    template['svg'], json.loads(json_results))
                result = self.exporter.text_to_path(infography)
                filename = t['export'].get(
                    'filename', '.'.join([str(uuid.uuid4()), t['export']['format']]))
                url = self.exporter.export(
                    result, filename, export_config)
                if 'notification' not in sub['subscription']:
                    _log.warning(
                        f'{t["user"]} notification configuration not found !')
                    continue
                notif_config = sub['subscription']['notification']['config']
                self.notifier.send_to_slack(
                    f'#{notif_config["channel"]}', t['name'], image_url=url, context=t['id'])
