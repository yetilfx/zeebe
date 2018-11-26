import React from 'react';
import {shallow} from 'enzyme';

import ComboBadge from './ComboBadge';

describe('ComboBadge', () => {
  it('should contain passed numbers', () => {
    const node = shallow(
      <ComboBadge>
        <ComboBadge.Left>1</ComboBadge.Left>
        <ComboBadge.Right>2</ComboBadge.Right>
      </ComboBadge>
    );
    expect(node.contains('1')).toBe(true);
    expect(node.contains('2')).toBe(true);
    expect(node).toMatchSnapshot();
  });

  it('should pass the right position', () => {
    const node = shallow(
      <ComboBadge>
        <ComboBadge.Left>1</ComboBadge.Left>
        <ComboBadge.Right>2</ComboBadge.Right>
      </ComboBadge>
    );

    expect(node.find(ComboBadge.Left).props().position).toBe(0);
    expect(node.find(ComboBadge.Right).props().position).toBe(1);
  });
});
