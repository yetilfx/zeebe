import {get} from 'modules/request';

export const fetchIncidentsByWorkflow = async () => {
  try {
    const response = await get('/api/incidents/byWorkflow');
    return {data: await response.json()};
  } catch (e) {
    return {error: e, data: []};
  }
};
