package com.hmdp.shopdetail.application;

import com.hmdp.dto.Result;
import com.hmdp.shopdetail.application.model.request.GuaranteeClaimCreateRequest;
import com.hmdp.shopdetail.application.service.ShopDetailApplicationService;
import com.hmdp.shopdetail.domain.model.GuaranteeClaim;
import com.hmdp.shopdetail.domain.service.ShopDetailDomainService;
import com.hmdp.shoprating.service.ShopRatingRefreshService;
import com.hmdp.shared.domain.compensation.model.FlashPayPayload;
import com.hmdp.shared.domain.compensation.service.FlashPayStateMachineFacade;
import com.hmdp.shared.stateengine.model.StateTask;
import com.hmdp.shared.stateengine.model.StateTaskStatus;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ShopDetailApplicationServiceResultMessageTest {

    /** 回写 result_message 超过列宽（255）会导致 Data truncation、申诉单卡死——必须先截断 */
    @Test
    void should_truncate_overlong_result_message_before_write_back() {
        ShopDetailDomainService domainService = mock(ShopDetailDomainService.class);
        ShopRatingRefreshService refreshService = mock(ShopRatingRefreshService.class);
        ShopDetailApplicationService service = new ShopDetailApplicationService(domainService, refreshService);

        FlashPayStateMachineFacade facade = mock(FlashPayStateMachineFacade.class);
        ReflectionTestUtils.setField(service, "flashPayStateMachineFacade", facade);

        GuaranteeClaim claim = new GuaranteeClaim();
        claim.setClaimNo("claim-t");
        claim.setShopId(1L);
        claim.setUserId(7L);
        claim.setPlanCode("FLASH_PAY");
        claim.setReason("r");
        when(domainService.getClaim("claim-t")).thenReturn(claim);
        when(domainService.createClaim(any(GuaranteeClaim.class))).thenAnswer(inv -> inv.getArgument(0));
        when(domainService.getClaim("claim-t")).thenReturn(claim);
        when(domainService.updateClaim(any(GuaranteeClaim.class)))
                .thenAnswer(inv -> inv.getArgument(0));

        StateTask task = StateTask.create("claim-t", "1", "FLASH_PAY", "MANUAL_REVIEW", "FLASH_PAY|r");
        String longMessage = "保存赔付审计异常: " + "x".repeat(400);
        task.markManualReview(longMessage);
        when(facade.start(eq("claim-t"), eq("1"), any(FlashPayPayload.class))).thenReturn(task);

        service.submitGuaranteeClaim(claim);

        org.mockito.ArgumentCaptor<GuaranteeClaim> captor =
                org.mockito.ArgumentCaptor.forClass(GuaranteeClaim.class);
        Mockito.verify(domainService).updateClaim(captor.capture());
        assertEquals(250, captor.getValue().getResultMessage().length());
        assertEquals(StateTaskStatus.MANUAL_REVIEW.name(), captor.getValue().getStatus());
    }
}
