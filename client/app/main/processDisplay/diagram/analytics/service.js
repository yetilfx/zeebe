import {dispatchAction} from 'view-utils';
import {updateOverlayVisibility, isBpmnType} from 'utils';
import {createSetElementAction} from './reducer';

export const BRANCH_OVERLAY = 'BRANCH_OVERLAY';

export function setEndEvent({id}) {
  dispatchAction(createSetElementAction(id, 'endEvent'));
}

export function unsetEndEvent() {
  dispatchAction(createSetElementAction(null, 'endEvent'));
}

export function setGateway({id}) {
  dispatchAction(createSetElementAction(id, 'gateway'));
}

export function unsetGateway() {
  dispatchAction(createSetElementAction(null, 'gateway'));
}

export function leaveGatewayAnalysisMode() {
  unsetGateway();
  unsetEndEvent();
}

export function hoverElement(viewer, element) {
  updateOverlayVisibility(viewer, element, BRANCH_OVERLAY);
}

export function addBranchOverlay(viewer, {flowNodes, piCount}) {
  const elementRegistry = viewer.get('elementRegistry');
  const overlays = viewer.get('overlays');

  Object.keys(flowNodes).forEach(element => {
    const value = flowNodes[element];

    if (value !== undefined && isBpmnType(elementRegistry.get(element), 'EndEvent')) {
      // create overlay node from html string
      const container = document.createElement('div');
      const percentageValue = Math.round(value / piCount * 1000) / 10;

      container.innerHTML =
      `<div class="tooltip top" role="tooltip" style="pointer-events: none; opacity: 0;">
        <table class="cam-table end-event-statistics">
          <tbody>
            <tr>
              <td>${piCount}</td>
              <td>Process Instances Total</td>
            </tr>
            <tr>
              <td>${value}</td>
              <td>Process Instances reached this state</td>
            </tr>
            <tr>
              <td>${percentageValue}%</td>
              <td>of Process Instances reached this state</td>
            </tr>
          </tbody>
        </table>
      </div>`;
      const overlayHtml = container.firstChild;

      // add overlay to viewer
      overlays.add(element, BRANCH_OVERLAY, {
        position: {
          bottom: 0,
          right: 0
        },
        show: {
          minZoom: -Infinity,
          maxZoom: +Infinity
        },
        html: overlayHtml
      });
    }
  });
}
